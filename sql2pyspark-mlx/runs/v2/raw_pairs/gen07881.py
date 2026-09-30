from pyspark.sql import functions as F

result = (
    df_inventory
    .filter(F.col("store_id").isin([1, 2, 5, 6]) & (F.col("store_id") == 4))
    .groupBy("product_id", "store_id")
    .agg(
        F.count("quantity").alias("quantity_count"),
        F.countDistinct("quantity").alias("unique_stores"),
    )
)
