from pyspark.sql import functions as F

src = (
    df_payments
    .filter(F.col("method") == "cash")
    .withColumn("paid_at_label", F.date_format("paid_at", "yyyy-MM"))
)

paid_month_summary = (
    src
    .groupBy(F.date_format("paid_at", "yyyy-MM").alias("paid_month"))
    .agg(
        F.sum(F.when(F.col("status") == "SUCCESS", 1).otherwise(0)).alias("success_count"),
    )
)

result = (
    paid_month_summary
    .filter(F.col("success_count") < 57)
    .select("paid_month", "success_count")
    .orderBy(F.col("success_count").desc())
)
