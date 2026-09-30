from pyspark.sql import functions as F

result = (
    df_order_items.alias("order_items")
    .join(df_products.alias("products"), F.col("order_items.product_id") == F.col("products.id"), "outer")
    .join(df_orders.alias("orders"), F.col("order_items.order_id") == F.col("orders.id"), "right")
    .groupBy(F.col("products.supplier_id"))
    .agg(
        (F.max(F.col("order_items.unit_price")) - F.min(F.col("order_items.unit_price"))).alias("unit_price_range"),
        F.sum((F.col("order_items.quantity") * F.col("order_items.unit_price"))).alias("revenue"),
    )
    .filter(F.col("unit_price_range") >= 64)
    .orderBy("supplier_id")
)
