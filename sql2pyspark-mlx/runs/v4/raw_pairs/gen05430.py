from pyspark.sql import functions as F

result = (
    df_products.alias("products")
    .join(df_order_items.alias("order_items"), F.col("products.id") == F.col("order_items.product_id"), "left_semi")
    .filter(F.col("products.cost") <= 710)
    .select(
        F.col("products.id"),
        F.col("products.category"),
        F.col("products.price"),
        F.col("products.stock_qty"),
    )
    .orderBy(F.col("id").desc())
)
