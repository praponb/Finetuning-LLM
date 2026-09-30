from pyspark.sql import functions as F

result = (
    df_inventory
    .filter(F.col("store_id").isin([3, 4]))
    .groupBy("store_id")
    .agg(
        F.countDistinct("quantity").alias("unique_stores"),
        F.max("quantity").alias("max_quantity"),
        F.min("last_restock").alias("min_last_restock"),
    )
    .filter(F.col("unique_stores") > 1)
)
