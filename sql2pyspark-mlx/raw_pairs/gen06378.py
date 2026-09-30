from pyspark.sql import functions as F

valid = (
    df_subscriptions
    .filter(F.col("customer_id").isNotNull())
    .select("*", F.round("monthly_fee", 0).alias("monthly_fee_rounded"))
)

result = (
    valid
    .groupBy("customer_id")
    .pivot("status", ["EXPIRED", "CANCELLED", "ACTIVE"])
    .agg(
        F.sum("monthly_fee_rounded").alias("total"),
        F.count(F.lit(1)).alias("item_count"),
    )
)
