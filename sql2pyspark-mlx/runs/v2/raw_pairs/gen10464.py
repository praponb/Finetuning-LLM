from pyspark.sql import functions as F

base = (
    df_inventory
    .filter(F.col("product_id").isin([1, 2, 6, 9]) & F.col("store_id").isin([3, 5]))
)

summary = (
    base
    .groupBy("store_id", "product_id")
    .agg(F.max("quantity").alias("max_quantity"))
)

result = (
    summary
    .filter(F.col("max_quantity") > 160)
    .select("store_id", "product_id", "max_quantity")
)
