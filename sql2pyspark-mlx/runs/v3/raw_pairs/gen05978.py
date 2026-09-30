from pyspark.sql import functions as F

result = (
    df_inventory
    .groupBy("store_id", "product_id")
    .agg(
        F.count("*").alias("num_inventory"),
        F.max("quantity").alias("max_quantity"),
    )
)
