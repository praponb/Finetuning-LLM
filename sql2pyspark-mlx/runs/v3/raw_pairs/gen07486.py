from pyspark.sql import functions as F

result = (
    df_inventory
    .groupBy("store_id")
    .agg(F.round(F.avg("quantity"), 2).alias("avg_quantity"))
    .orderBy(F.col("avg_quantity").desc())
)
