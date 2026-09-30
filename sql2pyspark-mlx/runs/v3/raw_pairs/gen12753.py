from pyspark.sql import functions as F

result = (
    df_inventory
    .groupBy("store_id", "product_id")
    .agg(
        F.max("quantity").alias("max_quantity"),
        F.count("*").alias("num_inventory"),
        F.count("quantity").alias("quantity_count"),
    )
    .filter(F.col("quantity_count") > 2)
)
