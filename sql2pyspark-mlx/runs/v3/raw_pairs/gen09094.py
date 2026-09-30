from pyspark.sql import functions as F

result = (
    df_orders
    .groupBy("status")
    .agg(
        F.sum(F.when(F.col("status") == "SHIPPED", F.col("amount")).otherwise(0)).alias("shipped_amount"),
        F.avg("amount").alias("avg_amount"),
    )
    .filter(F.col("shipped_amount") < 55)
)
