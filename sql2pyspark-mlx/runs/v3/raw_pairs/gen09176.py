from pyspark.sql import functions as F

result = (
    df_products.alias("p")
    .join(df_inventory.alias("i"), F.col("p.id") == F.col("i.product_id"), "left")
    .filter(F.col("i.quantity").between(144, 146) & F.col("i.store_id").isin([2, 4, 5, 6]))
    .select(F.col("p.brand"), F.col("p.launch_date"), F.col("i.last_restock"))
)
