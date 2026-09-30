from pyspark.sql import functions as F

result = (
    df_inventory
    .filter(F.col("product_id").isin([1, 7, 9]) | (F.col("store_id") == 4))
    .groupBy("product_id")
    .agg(
        F.count("quantity").alias("quantity_count"),
        F.max("quantity").alias("max_quantity"),
    )
    .filter(F.col("max_quantity") > 170)
    .drop("max_quantity")
)
