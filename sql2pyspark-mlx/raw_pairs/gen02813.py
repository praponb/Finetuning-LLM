from pyspark.sql import functions as F

result = (
    df_inventory
    .agg(
        F.round(F.avg("quantity"), 2).alias("avg_quantity"),
        F.sum("quantity").alias("total_quantity"),
    )
)
