from pyspark.sql import functions as F

result = (
    df_payments
    .groupBy("order_id")
    .agg(
        F.sum(F.when(F.col("status") == "SUCCESS", 1).otherwise(0)).alias("success_count"),
        F.count("amount").alias("amount_count"),
    )
    .filter(F.col("success_count") >= 40)
)
