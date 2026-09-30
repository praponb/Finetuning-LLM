from pyspark.sql import functions as F

result = (
    df_inventory
    .agg(
        F.round(F.avg("quantity"), 2).alias("avg_quantity"),
        F.count("quantity").alias("quantity_count"),
        F.min("quantity").alias("min_quantity"),
    )
    .orderBy("quantity_count")
)
