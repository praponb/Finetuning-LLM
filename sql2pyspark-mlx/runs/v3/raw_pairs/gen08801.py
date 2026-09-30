from pyspark.sql import functions as F

result = (
    df_order_items.alias("oi")
    .join(df_products.alias("p"), F.col("p.id") == F.col("oi.product_id"), "outer")
    .filter(F.col("oi.quantity") > 7)
    .select(
        F.col("oi.id"),
        F.col("oi.unit_price"),
        F.col("oi.order_id"),
        F.col("oi.quantity"),
        F.col("p.cost"),
        F.col("p.stock_qty"),
    )
    .orderBy(F.col("stock_qty").desc())
)
