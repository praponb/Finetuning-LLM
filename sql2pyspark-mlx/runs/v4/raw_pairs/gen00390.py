from pyspark.sql import functions as F

result = (
    df_inventory
    .groupBy("store_id", "product_id")
    .agg(F.round(F.avg("quantity"), 2).alias("avg_quantity"))
    .filter(F.col("avg_quantity") < 77)
    .orderBy(F.col("store_id").desc(), F.col("avg_quantity").desc())
)
