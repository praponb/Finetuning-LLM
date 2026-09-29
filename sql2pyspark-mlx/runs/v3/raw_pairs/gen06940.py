from pyspark.sql import functions as F

result = (
    df_order_items.alias("oi")
    .join(df_orders.alias("o"), F.col("oi.order_id") == F.col("o.id"), "inner")
    .join(df_products.alias("p"), F.col("oi.product_id") == F.col("p.id"), "outer")
    .filter(F.month(F.col("p.launch_date")).isin([1, 2, 9]))
    .select(
        F.col("oi.product_id"),
        F.col("oi.unit_price"),
        F.col("o.customer_id"),
        F.col("p.cost"),
        F.datediff(F.current_date(), F.col("p.launch_date")).alias("days_since_launch_date"),
    )
)
