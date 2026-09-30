from pyspark.sql import functions as F

result = (
    df_payments
    .groupBy(
        F.when(F.col("amount") >= 1240, "high").when(F.col("amount") >= 470, "mid").otherwise("low").alias("amount_bucket"),
        "status",
    )
    .agg(
        F.sum("amount").alias("total_amount"),
        F.sum(F.when(F.col("status") == "SUCCESS", 1).otherwise(0)).alias("success_count"),
    )
    .orderBy("amount_bucket")
)
