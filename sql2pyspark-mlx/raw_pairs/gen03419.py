from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("order_id").orderBy(F.col("paid_at").cast("bigint")).rangeBetween(-7776000, Window.currentRow)
w2 = Window.partitionBy("order_id")
w3 = Window.partitionBy("method")

kept_keys = (
    df_payments
    .select("method", "order_id")
    .subtract(
        df_payments
        .filter(F.col("status") == "SUCCESS")
        .select("method", "order_id")
    )
)

verified = (
    df_payments
    .alias("s")
    .join(kept_keys.alias("k"), (F.col("s.method") == F.col("k.method")) & (F.col("s.order_id") == F.col("k.order_id")), "inner")
    .select("s.*")
)

scored = (
    verified
    .withColumn("amount_count_90d", F.count("amount").over(w))
    .withColumn("amount_share", F.round((F.col("amount") / F.sum("amount").over(w2)), 4))
)

valid = (
    scored
    .filter(F.col("order_id").isNotNull())
    .select(
        "id",
        "method",
        "amount",
        "status",
        "amount_count_90d",
        "amount_share",
        F.length("method").alias("method_length"),
    )
)

filtered = (
    valid
    .filter(F.col("amount").between(420, 700))
    .select(
        "id",
        "method",
        "amount",
        "amount_count_90d",
        "amount_share",
        F.substring("status", 1, 2).alias("status_prefix"),
    )
)

scored_payments = (
    filtered
    .filter(F.col("amount_share") <= 0.29)
    .select(
        "*",
        F.stddev_samp("amount_count_90d").over(w3).alias("method_stddev_samp_amount_count_90d"),
    )
)

valid_payments = (
    scored_payments
    .filter(F.col("amount_count_90d") > 14)
    .withColumn("amount_share_level", F.when(F.col("amount_share") >= 0.76, "High").when(F.col("amount_share") >= 0.35, "Medium").otherwise("Low"))
    .withColumn("least_method_stddev_samp_amount_count_90d_amount_share", F.least("method_stddev_samp_amount_count_90d", "amount_share"))
    .withColumn("method_clean", F.coalesce("method", F.lit("OTHER")))
)

filtered_payments = (
    valid_payments
    .filter(F.col("method").isNotNull())
    .withColumn("least_method_stddev_samp_amount_count_90d_amount_share_scaled", F.round((F.col("least_method_stddev_samp_amount_count_90d_amount_share") * 1.15), 2))
)

result = (
    filtered_payments
    .groupBy(
        F.when(F.col("amount_share") >= 0.83, "PREMIUM").otherwise("STANDARD").alias("amount_share_tier"),
    )
    .agg(
        F.count(F.when(F.col("method") == "paypal", 1)).alias("paypal_rows"),
        F.sum(F.when(F.col("method") == "bank_transfer", F.col("method_stddev_samp_amount_count_90d")).otherwise(0)).alias("bank_transfer_method_stddev_samp_amount_count_90d"),
        F.percentile_approx("least_method_stddev_samp_amount_count_90d_amount_share_scaled", [0.25, 0.5, 0.75]).alias("least_method_stddev_samp_amount_count_90d_amount_share_scaled_quartiles"),
    )
    .orderBy("paypal_rows")
)
