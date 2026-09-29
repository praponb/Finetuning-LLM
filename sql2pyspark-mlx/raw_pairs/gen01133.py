from pyspark.sql import functions as F

result = (
    df_inventory
    .filter(F.col("product_id").isin([2, 5, 7, 8]) | F.col("product_id").isin([5, 7, 9]))
    .groupBy("product_id", "store_id")
    .agg(
        F.avg("quantity").alias("avg_quantity"),
        F.count("*").alias("num_inventory"),
    )
    .filter(F.col("avg_quantity") < 180)
)
