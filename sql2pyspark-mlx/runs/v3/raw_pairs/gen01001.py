from pyspark.sql import functions as F

result = (
    df_products.alias("products")
    .join(df_order_items.alias("order_items"), F.col("products.id") == F.col("order_items.product_id"), "inner")
    .filter(F.col("order_items.order_id").isin([15, 16, 38]) & (F.col("products.stock_qty") <= 198))
    .select(
        F.col("products.category"),
        F.col("order_items.order_id"),
        F.col("order_items.quantity"),
        F.col("order_items.unit_price"),
    )
    .orderBy("unit_price")
)
