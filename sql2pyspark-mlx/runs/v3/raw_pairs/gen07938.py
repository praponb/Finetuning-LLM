from pyspark.sql import functions as F

result = (
    df_products.alias("p")
    .join(df_order_items.alias("oi"), F.col("p.id") == F.col("oi.product_id"), "left")
    .select(
        F.col("p.cost"),
        F.col("p.launch_date"),
        F.col("p.stock_qty"),
        F.col("oi.product_id"),
        F.col("oi.order_id"),
        F.col("oi.unit_price"),
        F.col("oi.unit_price").cast("int").alias("unit_price_int"),
    )
    .orderBy("unit_price", F.col("unit_price_int").desc())
)
