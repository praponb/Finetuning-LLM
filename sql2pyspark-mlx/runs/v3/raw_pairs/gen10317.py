from pyspark.sql import functions as F

result = (
    df_order_items.alias("oi")
    .join(df_products.alias("p"), F.col("oi.product_id") == F.col("p.id"), "inner")
    .filter(F.col("p.launch_date") < "2023-11-01")
    .select(
        F.col("oi.id"),
        F.col("oi.unit_price"),
        F.col("oi.order_id"),
        F.col("oi.quantity"),
        F.col("p.product_name"),
        F.col("p.price"),
        F.greatest(F.col("p.cost"), F.col("p.stock_qty")).alias("greatest_cost_stock_qty"),
    )
)
