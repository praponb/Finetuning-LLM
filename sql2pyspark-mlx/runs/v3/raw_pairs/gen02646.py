from pyspark.sql import functions as F

result = (
    df_payments
    .groupBy("status")
    .agg(
        F.sum(F.when(F.col("status") == "REFUNDED", F.col("amount")).otherwise(0)).alias("refunded_amount"),
        F.avg("amount").alias("avg_amount"),
    )
    .filter(F.col("avg_amount") > 580)
    .select("status", "refunded_amount", "avg_amount")
    .orderBy(F.col("avg_amount").desc())
)
