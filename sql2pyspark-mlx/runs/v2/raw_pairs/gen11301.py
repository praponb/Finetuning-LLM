from pyspark.sql import functions as F

result = (
    df_payments
    .groupBy("status")
    .agg(
        F.round(F.avg("amount"), 2).alias("avg_amount"),
        F.sum(F.when(F.col("status") == "SUCCESS", F.col("amount")).otherwise(0)).alias("success_amount"),
    )
    .orderBy(F.col("avg_amount").desc(), "status")
    .limit(10)
)
