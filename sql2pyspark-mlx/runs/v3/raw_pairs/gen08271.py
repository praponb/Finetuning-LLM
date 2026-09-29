from pyspark.sql import functions as F

result = (
    df_inventory
    .filter((F.col("last_restock") > "2024-02-01") & (F.col("last_restock") < "2023-10-01"))
    .groupBy("product_id")
    .agg(F.round(F.avg("quantity"), 2).alias("avg_quantity"))
    .orderBy("product_id", F.col("avg_quantity").desc())
)
