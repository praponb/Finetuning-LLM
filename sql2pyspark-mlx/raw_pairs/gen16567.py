from pyspark.sql import functions as F

excluded_products = df_products.filter(F.col("supplier_id").isin([19, 22, 30, 37])).select("id")

result = (
    df_order_items.alias("order_items")
    .join(excluded_products.alias("products"), F.col("order_items.product_id") == F.col("products.id"), "left_anti")
    .filter(F.col("order_items.product_id").isNotNull() | F.lit(excluded_products.isEmpty()))
    .select(
        F.col("order_items.id"),
        F.col("order_items.order_id"),
        F.col("order_items.product_id"),
    )
)
