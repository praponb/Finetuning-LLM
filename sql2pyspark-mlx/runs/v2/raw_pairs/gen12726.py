from pyspark.sql import functions as F

result = (
    df_inventory
    .agg(
        F.min("quantity").alias("min_quantity"),
        F.avg("quantity").alias("avg_quantity"),
        F.count("quantity").alias("quantity_count"),
    )
    .orderBy(F.col("quantity_count").desc())
)
