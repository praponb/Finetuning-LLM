from pyspark.sql import functions as F

result = (
    df_inventory
    .agg(
        F.countDistinct("quantity").alias("unique_stores"),
        F.count("quantity").alias("quantity_count"),
        F.avg("quantity").alias("avg_quantity"),
    )
    .orderBy(F.col("avg_quantity").desc())
)
