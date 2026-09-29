from pyspark.sql import functions as F

result = (
    df_inventory.alias("inventory")
    .join(df_products.filter(F.col("cost").between(220, 300)).alias("products"), F.col("inventory.product_id") == F.col("products.id"), "left_semi")
    .filter(F.col("inventory.product_id").isin([1, 6, 9, 10]))
    .select(F.col("inventory.id"), F.col("inventory.last_restock"))
)
