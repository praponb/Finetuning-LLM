from pyspark.sql import functions as F

result = (
    df_order_items.alias("oi")
    .join(df_products.alias("p"), F.col("p.id") == F.col("oi.product_id"), "inner")
    .join(df_orders.alias("o"), F.col("oi.order_id") == F.col("o.id"), "inner")
    .select(
        F.col("oi.unit_price"),
        F.col("oi.quantity"),
        F.col("p.brand"),
        F.col("p.cost"),
        F.col("o.order_date"),
    )
    .orderBy("order_date")
)
