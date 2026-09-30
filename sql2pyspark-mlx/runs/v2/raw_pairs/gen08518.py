from pyspark.sql import functions as F

result = (
    df_products.alias("products")
    .join(df_order_items.filter(F.col("product_id").isin([14, 21, 34, 38])).alias("order_items"), F.col("products.id") == F.col("order_items.product_id"), "left_anti")
    .filter(F.col("products.supplier_id") == 39)
    .select(F.col("products.id"), F.col("products.brand"))
)
