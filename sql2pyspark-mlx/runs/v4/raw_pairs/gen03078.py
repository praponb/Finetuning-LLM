from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("channel")
w2 = Window.partitionBy("customer_id").orderBy("discount", "id")
w3 = Window.partitionBy("status", "customer_id").orderBy(F.col("amount").desc(), "id")
w4 = Window.partitionBy(F.col("s.id")).orderBy(F.col("c.paid_at").desc(), F.col("c.id").desc())

with_windows = (
    df_orders
    .withColumn("channel_stddev_samp_shipping_fee", F.stddev_samp("shipping_fee").over(w))
    .withColumn("next_shipping_fee", F.lead("shipping_fee", 1, 0).over(w2))
    .withColumn("row_seq", F.row_number().over(w3))
)

base = (
    with_windows
    .filter(F.col("customer_id").isNotNull())
    .select(
        "id",
        "customer_id",
        "store_id",
        "order_date",
        "status",
        "channel",
        "amount",
        "discount",
        "shipping_fee",
        "channel_stddev_samp_shipping_fee",
        "row_seq",
        F.coalesce("row_seq", F.lit(0)).alias("row_seq_filled"),
    )
)

with_latest = (
    base
    .alias("s")
    .join(df_payments.alias("c"), (F.col("c.order_id") == F.col("s.id")), "left")
    .withColumn("rn", F.row_number().over(w4))
    .filter(F.col("rn") == 1)
    .select("s.*", F.col("c.paid_at").alias("payment_paid_at"))
)

summary = (
    with_latest
    .withColumn("channel_stddev_samp_shipping_fee_tier", F.when(F.col("channel_stddev_samp_shipping_fee") >= 41, "PREMIUM").otherwise("STANDARD"))
    .cube("channel_stddev_samp_shipping_fee_tier")
    .agg(
        F.round(F.avg("channel_stddev_samp_shipping_fee"), 2).alias("avg_channel_stddev_samp_shipping_fee"),
        F.count(F.when(F.col("status") == "PENDING", 1)).alias("pending_count"),
        F.count(F.when(F.col("status") == "RETURNED", 1)).alias("returned_count"),
        F.percentile_approx("row_seq", 0.9).alias("row_seq_p90"),
        F.percentile_approx("row_seq", 0.95).alias("row_seq_p95"),
        F.grouping("channel_stddev_samp_shipping_fee_tier").alias("grouping_id"),
    )
)

result = (
    summary
    .select(
        "channel_stddev_samp_shipping_fee_tier",
        "avg_channel_stddev_samp_shipping_fee",
        "returned_count",
        "row_seq_p90",
    )
    .orderBy("returned_count", "row_seq_p90")
)
