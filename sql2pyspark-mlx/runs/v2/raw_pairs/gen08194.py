from pyspark.sql import functions as F

result = (
    df_products
    .groupBy("category")
    .agg(
        F.sum("price").alias("total_price"),
        F.max("launch_date").alias("last_launch_date"),
        F.sum(F.when(F.col("category") == "Fashion", 1).otherwise(0)).alias("fashion_count"),
    )
    .orderBy("total_price", "category")
    .limit(5)
)
