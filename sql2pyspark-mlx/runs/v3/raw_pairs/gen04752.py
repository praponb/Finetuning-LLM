from pyspark.sql import functions as F

result = (
    df_payments
    .filter(F.col("order_id") == 30)
    .agg(
        F.count("*").alias("total_count"),
        F.sum(F.when(F.col("status") == "SUCCESS", 1).otherwise(0)).alias("success_count"),
    )
)
