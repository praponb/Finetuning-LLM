from pyspark.sql import functions as F

result = (
    df_order_items.alias("oi")
    .join(df_products.alias("p"), F.col("p.id") == F.col("oi.product_id"), "inner")
    .filter((F.col("p.stock_qty") < 311) & F.col("oi.order_id").isin([12, 21, 26]))
    .select(F.col("oi.order_id"), F.col("p.brand"), F.col("p.launch_date"))
)
