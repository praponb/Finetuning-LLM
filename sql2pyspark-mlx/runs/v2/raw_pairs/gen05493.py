from pyspark.sql import functions as F

result = (
    df_inventory
    .filter((F.col("store_id") == 3) & (F.col("quantity") <= 135))
    .agg(F.round(F.avg("quantity"), 2).alias("avg_quantity"))
)
