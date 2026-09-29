from pyspark.sql import functions as F

recent_inventory = (
    df_inventory
    .filter(((F.col("quantity") <= 109) & F.col("quantity").between(72, 73)) | F.col("store_id").isin([1, 3, 6]))
)

agg = (
    recent_inventory
    .groupBy("product_id")
    .agg(F.avg("quantity").alias("avg_quantity"))
)

result = (
    agg
    .filter(F.col("avg_quantity") > 180)
    .orderBy(F.col("product_id").desc(), "avg_quantity")
)
