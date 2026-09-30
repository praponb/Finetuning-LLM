from pyspark.sql import functions as F

base = (
    df_inventory
    .filter((F.col("quantity").isNotNull() & (F.col("quantity") >= 233)) | F.col("quantity").between(91, 94))
)

agg = (
    base
    .groupBy("store_id", "product_id")
    .agg(F.sum("quantity").alias("total_quantity"))
)

result = agg.select("store_id", "product_id", "total_quantity")
