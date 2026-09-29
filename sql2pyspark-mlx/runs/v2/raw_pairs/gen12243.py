from pyspark.sql import functions as F

result = (
    df_order_items.alias("oi")
    .join(df_products.alias("p"), F.col("oi.product_id") == F.col("p.id"), "inner")
    .join(df_orders.alias("o"), F.col("oi.order_id") == F.col("o.id"), "inner")
    .filter(F.col("o.status").isin(["CANCELLED", "RETURNED"]) & (F.col("p.launch_date") < "2024-05-02"))
    .select(
        F.col("oi.quantity"),
        F.col("oi.unit_price"),
        F.col("p.price"),
        F.col("p.brand"),
        F.col("o.order_date"),
    )
)
