from pyspark.sql import functions as F

result = (
    df_payments
    .filter(F.col("amount").between(620, 700))
    .groupBy(
        F.when(F.col("amount") >= 1400, "high").when(F.col("amount") >= 710, "mid").otherwise("low").alias("amount_bucket"),
    )
    .agg(
        F.sum(F.when(F.col("status") == "SUCCESS", 1).otherwise(0)).alias("success_count"),
    )
    .orderBy("success_count")
)
