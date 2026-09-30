from pyspark.sql import functions as F

recent_inventory = (
    df_inventory
    .filter(F.col("quantity").isNotNull() & (F.year("last_restock") == 2025))
    .withColumn("quantity_level", F.when(F.col("quantity") >= 201, "High").when(F.col("quantity") >= 78, "Medium").otherwise("Low"))
)

store_id_summary = (
    recent_inventory
    .groupBy("store_id", "product_id")
    .agg(F.min("last_restock").alias("first_last_restock"))
)

result = (
    store_id_summary
    .select("store_id", "product_id", "first_last_restock")
    .orderBy(F.col("product_id").desc())
)
