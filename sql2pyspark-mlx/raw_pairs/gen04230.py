from pyspark.sql import functions as F

result = (
    df_inventory
    .groupBy("product_id", "store_id")
    .agg(F.round(F.avg("quantity"), 2).alias("avg_quantity"))
    .filter(F.col("avg_quantity") > 27)
    .select("product_id", "store_id", "avg_quantity")
    .orderBy(F.col("product_id").desc())
)
