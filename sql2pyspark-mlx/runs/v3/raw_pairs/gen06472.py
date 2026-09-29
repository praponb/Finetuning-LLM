from pyspark.sql import functions as F

result = (
    df_inventory.alias("inventory")
    .join(df_products.alias("products"), F.col("inventory.product_id") == F.col("products.id"), "left")
    .join(df_suppliers.alias("suppliers"), F.col("suppliers.id") == F.col("products.supplier_id"), "inner")
    .filter((F.col("inventory.quantity") == 148) | (F.col("inventory.quantity") < 239))
    .select(
        F.col("inventory.id"),
        F.col("inventory.store_id"),
        F.col("inventory.product_id"),
        F.col("products.cost"),
        F.col("products.category"),
        F.col("suppliers.city"),
        F.col("suppliers.rating"),
    )
)
