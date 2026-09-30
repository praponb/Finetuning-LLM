from pyspark.sql import functions as F

result = (
    df_inventory
    .filter(F.col("quantity") <= 66)
    .groupBy("store_id", "product_id")
    .agg(
        F.count("quantity").alias("quantity_count"),
        F.max("quantity").alias("max_quantity"),
        F.min("quantity").alias("min_quantity"),
    )
    .filter(F.col("min_quantity") > 190)
    .drop("min_quantity")
)
