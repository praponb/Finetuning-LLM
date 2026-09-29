from pyspark.sql import functions as F

result = (
    df_inventory.alias("inventory")
    .join(df_products.alias("products"), F.col("inventory.product_id") == F.col("products.id"), "inner")
    .groupBy(F.col("products.category"))
    .agg(F.sum(F.col("inventory.quantity")).alias("total_quantity"))
)
