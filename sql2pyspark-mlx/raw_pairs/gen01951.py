from pyspark.sql import functions as F

result = (
    df_order_items.alias("oi")
    .join(df_products.alias("p"), F.col("oi.product_id") == F.col("p.id"), "inner")
    .filter((F.col("p.price") > 420) & F.col("oi.product_id").isin([5, 36]))
    .select(
        F.col("oi.id"),
        F.col("oi.order_id"),
        F.col("p.launch_date"),
        (F.col("p.stock_qty") - F.col("p.price")).alias("stock_qty_minus_price"),
    )
)
