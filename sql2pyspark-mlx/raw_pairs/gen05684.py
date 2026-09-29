from pyspark.sql import functions as F

result = (
    df_products.alias("p")
    .join(df_order_items.alias("oi"), F.col("p.id") == F.col("oi.product_id"), "inner")
    .filter(F.col("p.price").between(840, 900))
    .select(
        F.col("p.stock_qty"),
        F.col("p.brand"),
        F.col("p.launch_date"),
        F.col("oi.unit_price"),
    )
    .orderBy(F.col("unit_price").desc())
)
