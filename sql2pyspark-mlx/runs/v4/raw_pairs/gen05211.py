from pyspark.sql import functions as F

result = (
    df_payments
    .filter(F.hour("paid_at").between(11, 15))
    .groupBy(F.year("paid_at").alias("paid_year"))
    .agg(
        F.count(F.when(F.col("status") == "FAILED", 1)).alias("failed_count"),
        F.avg("amount").alias("avg_amount"),
    )
    .filter(F.col("avg_amount") < 760)
    .orderBy("failed_count", F.col("paid_year").desc())
)
