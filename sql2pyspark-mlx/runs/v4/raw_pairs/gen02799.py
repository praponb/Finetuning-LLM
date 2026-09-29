from pyspark.sql import functions as F

result = (
    df_inventory
    .filter(F.col("store_id").isin([2, 3, 6]))
    .agg(
        F.countDistinct("quantity").alias("unique_stores"),
        F.sum("quantity").alias("total_quantity"),
    )
    .orderBy(F.col("unique_stores").desc())
)
