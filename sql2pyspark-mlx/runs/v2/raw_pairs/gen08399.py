from pyspark.sql import functions as F

result = (
    df_products.alias("products")
    .join(df_order_items.alias("order_items"), F.col("products.id") == F.col("order_items.product_id"), "left_semi")
    .select(F.col("products.id"), F.col("products.product_name"))
    .orderBy(F.col("product_name").desc(), "id")
    .limit(50)
)
