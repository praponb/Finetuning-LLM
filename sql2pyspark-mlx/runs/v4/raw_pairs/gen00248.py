from pyspark.sql import functions as F

result = (
    df_inventory
    .groupBy("store_id", F.quarter("last_restock").alias("last_restock_quarter"))
    .agg(
        F.min("quantity").alias("min_quantity"),
        F.round(F.avg("quantity"), 2).alias("avg_quantity"),
        F.count("*").alias("row_count"),
        F.countDistinct("quantity").alias("unique_products"),
    )
    .filter(F.col("unique_products") > 4)
    .drop("unique_products")
    .orderBy("store_id")
)
