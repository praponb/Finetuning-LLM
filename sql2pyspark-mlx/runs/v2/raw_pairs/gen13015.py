from pyspark.sql import functions as F

result = (
    df_orders
    .agg(
        F.sum(F.when(F.col("status") == "SHIPPED", 1).otherwise(0)).alias("shipped_count"),
        F.max("discount").alias("max_discount"),
    )
)
