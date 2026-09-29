from pyspark.sql import functions as F

result = (
    df_order_items.alias("oi")
    .join(df_products.alias("p"), F.col("oi.product_id") == F.col("p.id"), "left")
    .select(
        F.col("oi.id"),
        F.col("oi.product_id"),
        F.col("p.brand"),
        F.col("p.price"),
        F.col("p.stock_qty"),
        F.round(F.col("oi.unit_price"), 1).alias("unit_price_rounded"),
    )
)
