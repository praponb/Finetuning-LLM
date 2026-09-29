from pyspark.sql import functions as F

result = (
    df_order_items.alias("order_items")
    .join(df_products.alias("products"), F.col("order_items.product_id") == F.col("products.id"), "inner")
    .filter(F.col("products.stock_qty") == 362)
    .groupBy(F.col("products.supplier_id"))
    .agg(
        (F.max(F.col("order_items.unit_price")) - F.min(F.col("order_items.unit_price"))).alias("unit_price_range"),
    )
)
