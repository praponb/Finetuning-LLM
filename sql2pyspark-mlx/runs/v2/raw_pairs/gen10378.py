from pyspark.sql import functions as F

valid_payments = (
    df_payments
    .filter(F.col("method") == "cash")
    .withColumn("is_success", F.when(F.col("status") == "SUCCESS", 1).otherwise(0))
)

agg = (
    valid_payments
    .groupBy("status", F.year("paid_at").alias("paid_year"))
    .agg(
        F.round(((100 * F.sum(F.when(F.col("status") == "FAILED", 1).otherwise(0))) / F.count("*")), 1).alias("pct_failed"),
        F.count("is_success").alias("is_success_count"),
    )
)

result = (
    agg
    .select("status", "paid_year", "pct_failed", "is_success_count")
    .orderBy(F.col("pct_failed").desc(), "paid_year")
)
