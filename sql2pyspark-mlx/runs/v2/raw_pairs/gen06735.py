from pyspark.sql import functions as F

result = (
    df_products.alias("products")
    .join(df_inventory.alias("inventory"), F.col("products.id") == F.col("inventory.product_id"), "left")
    .filter(F.col("inventory.store_id").isin([2, 5]) & F.col("inventory.quantity").between(141, 195))
    .select(F.col("products.cost"), F.col("inventory.quantity"))
    .orderBy("cost")
)
