from pyspark.sql import functions as F

result = (
    df_order_items.alias("oi")
    .join(df_products.alias("p"), F.col("p.id") == F.col("oi.product_id"), "right")
    .join(df_orders.alias("o"), F.col("oi.order_id") == F.col("o.id"), "inner")
    .filter(F.col("o.amount").between(530, 1380))
    .select(
        F.col("oi.unit_price"),
        F.col("p.launch_date"),
        F.col("p.cost"),
        F.col("o.amount"),
    )
)
