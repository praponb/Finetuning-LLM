from pyspark.sql import functions as F

result = (
    df_inventory.alias("inventory")
    .join(df_products.filter(F.col("category").isin(["Grocery", "Home", "Books"])).alias("products"), F.col("inventory.product_id") == F.col("products.id"), "left_anti")
    .filter(F.month(F.col("inventory.last_restock")).isin([5, 7, 11]))
    .select(
        F.col("inventory.id"),
        F.col("inventory.quantity"),
        F.col("inventory.product_id"),
    )
    .orderBy("id")
)
