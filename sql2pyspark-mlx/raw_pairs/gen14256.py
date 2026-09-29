from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("status").orderBy(F.col("amount").desc_nulls_last(), "id")
w2 = Window.partitionBy("customer_id").orderBy("amount")
w3 = Window.partitionBy(F.col("s.id"), F.col("s.subscription_id")).orderBy(F.col("c.quantity").desc(), F.col("c.id").desc())
w4 = Window.partitionBy(F.col("s.id"), F.col("s.subscription_id")).orderBy(F.col("c.cost").desc(), F.col("c.id").desc())
w5 = Window.partitionBy("store_id").orderBy("monthly_fee", "id", "subscription_id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w6 = Window.partitionBy("store_id").orderBy(F.col("monthly_fee").asc_nulls_first())

latest = (
    df_orders
    .withColumn("rn", F.row_number().over(w))
    .filter(F.col("rn") == 1)
)

matched = (
    latest
    .alias("src")
    .join(df_subscriptions.alias("sub"), (F.col("src.customer_id") == F.col("sub.customer_id")) & (F.col("src.order_date") >= F.col("sub.start_date")) & (F.col("src.order_date") < F.col("sub.end_date") + F.expr("INTERVAL 7 DAYS")), "left")
    .select(
        "src.*",
        F.col("sub.id").alias("subscription_id"),
        F.col("sub.plan"),
        F.col("sub.monthly_fee"),
    )
)

kept = (
    matched
    .alias("s")
    .join(df_payments.alias("x"), (F.col("x.order_id") == F.col("s.id")) & (F.col("x.order_id") == 1), "left_semi")
)

ranked = (
    kept
    .withColumn("rn_2", F.rank().over(w2))
    .filter(F.col("rn_2") == 1)
)

latest_match = (
    ranked
    .alias("s")
    .join(df_order_items.alias("c"), (F.col("c.order_id") == F.col("s.id")), "left")
    .withColumn("rn_3", F.row_number().over(w3))
    .filter(F.col("rn_3") == 1)
    .select("s.*", F.col("c.quantity"), F.col("c.unit_price"))
)

latest_match_orders = (
    latest_match
    .alias("s")
    .join(df_shipments.alias("c"), (F.col("c.order_id") == F.col("s.id")), "left")
    .withColumn("rn_3", F.row_number().over(w4))
    .filter(F.col("rn_3") == 1)
    .select(
        "s.id",
        "s.store_id",
        "s.status",
        "s.channel",
        "s.amount",
        "s.discount",
        "s.rn",
        "s.subscription_id",
        "s.plan",
        "s.monthly_fee",
        "s.rn_2",
        F.col("c.status").alias("shipment_status"),
        F.col("c.delivered_date").alias("shipment_delivered_date"),
    )
)

enriched = (
    latest_match_orders
    .withColumn("cumulative_monthly_fee", F.sum("monthly_fee").over(w5))
    .withColumn("monthly_fee_cume_dist", F.cume_dist().over(w6))
)

result = (
    enriched
    .alias("src")
    .join(df_stores.alias("p"), F.col("src.store_id") == F.col("p.id"), "inner")
    .rollup(
        F.col("p.region").alias("region"),
        F.col("p.city").alias("city"),
        F.col("src.channel").alias("channel"),
    )
    .agg(
        F.count("*").alias("cnt"),
        F.avg(F.col("src.amount")).alias("avg_amount"),
    )
    .orderBy(F.col("city").desc())
)
