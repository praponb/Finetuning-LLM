from pyspark.sql import functions as F

result = (
    df_inventory
    .filter((F.col("quantity") <= 122) & F.col("store_id").isin([3, 6]) & F.col("product_id").isin([6, 7, 8]))
    .groupBy("store_id")
    .agg(
        F.sum("quantity").alias("total_quantity"),
        F.max("quantity").alias("max_quantity"),
    )
    .filter(F.col("max_quantity") < 170)
    .drop("max_quantity")
    .orderBy("total_quantity", "store_id")
)
