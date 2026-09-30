from pyspark.sql import functions as F

result = (
    df_inventory
    .filter(~(((F.col("quantity") == 182) & (F.year("last_restock") == 2025)) | (F.col("last_restock") < "2023-04-01")))
    .agg(
        F.avg("quantity").alias("avg_quantity"),
        F.min("quantity").alias("min_quantity"),
        F.round(F.stddev("quantity"), 2).alias("stddev_quantity"),
    )
)
