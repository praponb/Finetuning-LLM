from pyspark.sql import functions as F

result = (
    df_products.alias("products")
    .join(df_inventory.alias("inventory"), F.col("products.id") == F.col("inventory.product_id"), "left_semi")
    .filter(F.col("products.stock_qty").between(229, 385))
    .select(
        F.col("products.id"),
        F.col("products.brand"),
        F.col("products.price"),
        F.col("products.cost"),
    )
)
