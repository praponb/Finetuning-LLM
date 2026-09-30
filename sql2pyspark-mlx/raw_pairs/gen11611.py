from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("customer_id").orderBy("shipping_fee", "id")
w2 = Window.partitionBy(F.col("s.id")).orderBy(F.col("c.paid_at").desc(), F.col("c.id").desc())
w3 = Window.partitionBy("store_id", "customer_id").orderBy(F.col("shipping_fee").asc_nulls_first(), "id")

valid = (
    df_orders
    .filter(F.col("channel") != "partner")
    .select(
        "id",
        "customer_id",
        "store_id",
        "order_date",
        "status",
        "channel",
        "shipping_fee",
        F.upper("channel").alias("channel_upper"),
    )
)

ranked = (
    valid
    .withColumn("latest_rank", F.row_number().over(w))
    .filter(F.col("latest_rank") == 1)
    .drop("latest_rank")
)

with_latest = (
    ranked
    .alias("s")
    .join(df_payments.alias("c"), (F.col("c.order_id") == F.col("s.id")) & (F.col("c.paid_at") <= F.col("s.order_date")), "left")
    .withColumn("rn", F.row_number().over(w2))
    .filter(F.col("rn") == 1)
    .select("s.*", F.col("c.amount"))
)

kept = (
    with_latest
    .alias("s")
    .join(df_shipments.alias("x"), (F.col("x.order_id") == F.col("s.id")) & (F.col("x.delivered_date") < F.col("s.order_date")), "left_semi")
)

latest = (
    kept
    .withColumn("rn", F.row_number().over(w3))
    .filter(F.col("rn") == 1)
    .select("id", "customer_id", "order_date", "status", "shipping_fee", "amount")
)

result = (
    latest
    .alias("e")
    .join(df_customers.alias("p"), F.col("e.customer_id") == F.col("p.id"), "inner")
    .rollup(F.col("p.city").alias("city"), F.col("e.status").alias("status"))
    .agg(
        F.count("*").alias("rows_total"),
        F.avg(F.col("e.shipping_fee")).alias("avg_shipping_fee"),
        F.sum(F.col("e.amount")).alias("total_amount"),
        F.grouping_id().alias("level_id"),
    )
)
