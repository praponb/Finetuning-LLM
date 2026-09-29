from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("plan", "status").orderBy(F.col("discount").desc(), "id", "subscription_id")
w2 = Window.partitionBy(F.col("s.id"), F.col("s.subscription_id")).orderBy(F.col("c.cost").desc(), F.col("c.id").desc())

filtered = (
    df_orders
    .filter((F.col("order_date") >= F.lit("2023-06-16").cast("date")) & (F.col("amount") > 1150))
    .select(
        "*",
        F.when(F.col("amount") >= 1710, "High").when(F.col("amount") >= 220, "Medium").otherwise("Low").alias("amount_band"),
    )
)

attributed = (
    filtered
    .alias("s")
    .join(df_subscriptions.alias("sub"), (F.col("s.customer_id") == F.col("sub.customer_id")) & (F.col("s.order_date") >= F.col("sub.start_date")) & (F.col("s.order_date") < F.col("sub.end_date") + F.expr("INTERVAL 30 DAYS")), "left")
    .select(
        "s.*",
        F.col("sub.id").alias("subscription_id"),
        F.col("sub.plan"),
        F.col("sub.monthly_fee"),
    )
)

kept = (
    attributed
    .alias("s")
    .join(df_order_items.alias("ch"), (F.col("ch.order_id") == F.col("s.id")) & (F.col("ch.unit_price").between(210, 400)), "left_anti")
)

kept_orders = (
    kept
    .alias("s")
    .join(df_shipments.alias("x"), (F.col("x.order_id") == F.col("s.id")) & (F.col("x.shipped_date") <= "2025-05-01"), "left_semi")
)

eligible = (
    kept_orders
    .alias("s")
    .join(df_shipments.alias("ch"), (F.col("ch.order_id") == F.col("s.id")) & (F.col("ch.shipped_date") >= F.col("s.order_date")), "left_anti")
)

deduped = (
    eligible
    .withColumn("row_num", F.row_number().over(w))
    .filter(F.col("row_num") == 1)
    .select("id", "discount", "subscription_id", "monthly_fee")
)

with_last = (
    deduped
    .alias("s")
    .join(df_shipments.alias("c"), (F.col("c.order_id") == F.col("s.id")), "inner")
    .withColumn("rn", F.row_number().over(w2))
    .filter(F.col("rn") == 1)
    .select("s.*", F.col("c.delivered_date"))
)

result = (
    with_last
    .groupBy(
        F.date_trunc("WEEK", "delivered_date").alias("week_start"),
        "subscription_id",
    )
    .agg(
        F.count("*").alias("event_count"),
        F.round(F.avg("monthly_fee"), 2).alias("avg_monthly_fee"),
        F.avg("discount").alias("avg_discount"),
        F.count("*").alias("cnt"),
        F.min("discount").alias("min_discount"),
    )
)
