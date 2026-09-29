from pyspark.sql import functions as F

base = df_inventory.filter(F.col("quantity") != 96)

summary = (
    base
    .groupBy("store_id")
    .agg(F.count("quantity").alias("quantity_count"))
)

result = (
    summary
    .filter(F.col("quantity_count") >= 5)
    .select("store_id", "quantity_count")
    .orderBy(F.col("store_id").desc(), F.col("quantity_count").desc())
)
