from pyspark.sql import functions as F

result = (
    df_inventory
    .groupBy("store_id")
    .agg(
        F.count("quantity").alias("quantity_count"),
        F.min("quantity").alias("min_quantity"),
        F.sum("quantity").alias("total_quantity"),
    )
    .filter(F.col("total_quantity") >= 1440)
)
