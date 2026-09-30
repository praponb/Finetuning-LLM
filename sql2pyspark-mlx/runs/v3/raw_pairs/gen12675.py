from pyspark.sql import functions as F

recent_inventory = df_inventory.filter(F.col("last_restock").between("2024-06-01", "2024-12-06"))

summary = (
    recent_inventory
    .groupBy("product_id")
    .agg(F.count("quantity").alias("quantity_count"))
)

result = (
    summary
    .filter(F.col("quantity_count") < 4)
    .select("product_id", "quantity_count")
    .orderBy(F.col("product_id").desc(), "quantity_count")
)
