from pyspark.sql import functions as F

result = (
    df_payments
    .groupBy(F.quarter("paid_at").alias("paid_quarter"))
    .agg(
        F.count(F.when(F.col("status") == "SUCCESS", 1)).alias("success_count"),
        F.round(F.avg("amount"), 2).alias("avg_amount"),
    )
    .filter(F.col("avg_amount") >= 51)
    .select("paid_quarter", "success_count", "avg_amount")
    .orderBy(F.col("paid_quarter").desc())
)
