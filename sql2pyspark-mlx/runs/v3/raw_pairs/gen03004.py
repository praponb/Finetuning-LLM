from pyspark.sql import functions as F

valid_inventory = df_inventory.filter(F.col("quantity").between(131, 141))

product_id_summary = (
    valid_inventory
    .groupBy("product_id")
    .agg(
        F.min("last_restock").alias("min_last_restock"),
        F.count("quantity").alias("quantity_count"),
    )
)

result = (
    product_id_summary
    .filter(F.col("quantity_count") >= 9)
    .select("product_id", "min_last_restock", "quantity_count")
)
