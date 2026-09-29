from pyspark.sql import functions as F

base = (
    df_payments
    .filter(F.col("order_id").isin([2, 40]) & (F.col("order_id") == 33))
    .withColumn("clean_method", F.trim("method"))
)

agg = (
    base
    .groupBy(
        F.when(F.year("paid_at") < 2024, "before_2024").when(F.year("paid_at") == 2024, "2024").otherwise("after_2024").alias("paid_cohort"),
        "order_id",
    )
    .agg(
        F.sum(F.when(F.col("status") == "FAILED", 1).otherwise(0)).alias("failed_count"),
        F.count(F.when(F.col("status") == "SUCCESS", 1)).alias("success_count"),
    )
)

result = (
    agg
    .filter(F.col("success_count") >= 7)
    .select("paid_cohort", "failed_count", "success_count")
)
