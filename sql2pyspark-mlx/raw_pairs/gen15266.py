from pyspark.sql import functions as F

result = (
    df_payments
    .groupBy("status")
    .agg(
        F.max("paid_at").alias("latest_paid_at"),
        F.sum("amount").alias("total_amount"),
    )
    .filter(F.col("total_amount") >= 7300)
    .select("status", "latest_paid_at", "total_amount")
    .orderBy(F.col("status").desc())
    .limit(5)
)
