from pyspark.sql import functions as F

result = (
    df_inventory
    .groupBy("store_id", "product_id")
    .agg(F.avg("quantity").alias("avg_quantity"))
    .filter(F.col("avg_quantity") > 210)
    .select("store_id", "product_id", "avg_quantity")
    .orderBy(F.col("avg_quantity").desc())
)
