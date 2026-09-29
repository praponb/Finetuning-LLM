from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("method").orderBy("is_promptpay", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w2 = Window.partitionBy("method", "status").orderBy("is_promptpay", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w3 = Window.partitionBy("method", "order_id").orderBy("paid_at", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w4 = Window.partitionBy("status")
w5 = Window.partitionBy("status").orderBy(F.col("paid_at").cast("bigint")).rangeBetween(-7776000, Window.currentRow)
w6 = Window.partitionBy("order_id").orderBy(F.col("paid_at").cast("bigint")).rangeBetween(-2592000, Window.currentRow)
w7 = Window.partitionBy("order_id")
w8 = Window.partitionBy("status").orderBy(F.col("running_amount_sum_90d").desc(), "id")
w9 = Window.partitionBy("order_id").orderBy("running_amount_share", "id")

filtered = (
    df_payments
    .filter(F.col("method") == "bank_transfer")
    .select(
        "id",
        "order_id",
        "method",
        "amount",
        "paid_at",
        "status",
        F.when(F.col("method") == "promptpay", 1).otherwise(0).alias("is_promptpay"),
    )
)

metrics = (
    filtered
    .select(
        "*",
        F.last("amount", ignorenulls=True).over(w).alias("last_known_amount"),
        F.sum("is_promptpay").over(w2).alias("running_is_promptpay"),
        F.sum("amount").over(w3).alias("running_amount"),
        F.round((F.col("amount") / F.sum("amount").over(w4)), 4).alias("amount_share"),
    )
)

valid = (
    metrics
    .filter((F.col("paid_at") >= F.lit("2024-04-25 18:00:00").cast("timestamp")) & (F.col("method") == "bank_transfer"))
    .withColumn("status_prefix", F.substring("status", 1, 3))
    .withColumn("method_clean", F.coalesce("method", F.lit("OTHER")))
)

cleaned = (
    valid
    .filter(F.col("amount_share").between(0.42, 0.75))
    .select(
        "id",
        "order_id",
        "amount",
        "paid_at",
        "status",
        "is_promptpay",
        "last_known_amount",
        "running_is_promptpay",
        "running_amount",
        "amount_share",
        "status_prefix",
        "method_clean",
        F.concat_ws("-", "method", "status").alias("method_status_key"),
        F.coalesce("running_amount", F.lit(0)).alias("running_amount_filled"),
    )
)

metrics_payments = (
    cleaned
    .select(
        "*",
        F.sum("running_amount").over(w5).alias("running_amount_sum_90d"),
        F.sum("amount_share").over(w6).alias("amount_share_sum_30d"),
        F.round((F.col("running_amount") / F.sum("running_amount").over(w7)), 4).alias("running_amount_share"),
    )
)

enriched = (
    metrics_payments
    .select(
        "*",
        F.ntile(10).over(w8).alias("running_amount_sum_90d_bucket"),
        F.round((F.col("running_amount_sum_90d") / F.sum("running_amount_sum_90d").over(w4)), 4).alias("running_amount_sum_90d_share"),
        F.lag("last_known_amount", 2).over(w9).alias("prev_last_known_amount"),
    )
)

result = (
    enriched
    .groupBy(
        F.when(F.col("running_amount_filled") >= 61, "HIGH").otherwise("LOW").alias("running_amount_filled_tier"),
    )
    .agg(
        ((F.unix_timestamp(F.max("paid_at")) - F.unix_timestamp(F.min("paid_at"))) / 60).alias("duration_min"),
        F.percentile_approx("amount_share_sum_30d", 0.9).alias("amount_share_sum_30d_p90"),
        F.count(F.when(F.col("status") == "REFUNDED", 1)).alias("refunded_count"),
        F.count_if(F.col("status") == "REFUNDED").alias("refunded_events"),
        F.countDistinct("status").alias("distinct_status"),
        F.sum(F.when(F.col("status") == "REFUNDED", F.col("amount_share")).otherwise(0)).alias("refunded_amount_share"),
    )
    .filter(F.col("refunded_count") < 1)
    .orderBy(F.col("refunded_events").asc_nulls_last())
)
