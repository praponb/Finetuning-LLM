from pyspark.sql import functions as F

result = (
    df_products.alias("products")
    .join(df_inventory.filter(F.col("quantity") >= 108).alias("inventory"), F.col("products.id") == F.col("inventory.product_id"), "left_semi")
    .select(F.col("products.id"), F.col("products.brand"), F.col("products.stock_qty"))
    .orderBy(F.col("id").desc())
)
