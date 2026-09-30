from pyspark.sql import functions as F

result = (
    df_orders
    .groupBy("status")
    .agg(
        F.count("*").alias("num_orders"),
        F.count(F.when(F.col("status") == "SHIPPED", 1)).alias("shipped_count"),
    )
    .filter(F.col("shipped_count") >= 5)
    .drop("shipped_count")
)
