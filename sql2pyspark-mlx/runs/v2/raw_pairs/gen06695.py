from pyspark.sql import functions as F

result = (
    df_order_items.alias("order_items")
    .join(df_products.alias("products"), F.col("order_items.product_id") == F.col("products.id"), "inner")
    .join(df_orders.alias("orders"), F.col("orders.id") == F.col("order_items.order_id"), "inner")
    .filter(F.col("products.cost").between(490, 510))
    .select(
        F.col("order_items.id"),
        F.col("order_items.order_id"),
        F.col("order_items.unit_price"),
        F.col("products.supplier_id"),
        F.col("products.product_name"),
        F.col("orders.discount"),
        F.col("orders.customer_id"),
        F.datediff(F.current_date(), F.col("orders.order_date")).alias("days_since_order_date"),
    )
)
