from pyspark.sql import functions as F

result = (
    df_inventory
    .filter(F.col("last_restock") < "2024-04-01")
    .groupBy("store_id")
    .agg(
        F.countDistinct("quantity").alias("unique_products"),
        F.max("quantity").alias("max_quantity"),
    )
)
