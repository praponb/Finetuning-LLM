from pyspark.sql import functions as F

valid_inventory = df_inventory.filter(F.col("quantity").between(189, 229))

summary = (
    valid_inventory
    .groupBy("product_id")
    .agg(F.count("quantity").alias("quantity_count"))
)

result = summary.select("product_id", "quantity_count")
