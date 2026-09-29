from pyspark.sql import functions as F

filtered = (
    df_orders
    .filter((~F.col("channel").eqNullSafe(F.lit("partner"))) & F.col("customer_id").isNotNull())
    .select(
        "*",
        (F.col("discount") - F.col("amount")).alias("discount_minus_amount"),
        F.round((F.col("discount") * 0.85), 2).alias("discount_adjusted"),
        F.coalesce("channel", F.lit("DIRECT")).alias("channel_filled"),
    )
)

result = (
    filtered
    .groupBy("status")
    .agg(
        F.sum(F.when(F.col("status") == "PENDING", F.col("amount")).otherwise(0)).alias("pending_amount"),
        F.percentile_approx("shipping_fee", [0.25, 0.5, 0.75]).alias("shipping_fee_quartiles"),
        F.countDistinct("id").alias("distinct_ids"),
    )
)
