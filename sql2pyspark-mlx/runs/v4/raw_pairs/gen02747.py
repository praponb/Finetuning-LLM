from pyspark.sql import functions as F

result = (
    df_products.alias("p")
    .join(df_order_items.filter(F.col("product_id").isin([28, 36])).alias("oi"), F.col("p.id") == F.col("oi.product_id"), "left_anti")
    .select(
        F.col("p.id"),
        F.col("p.product_name"),
        F.col("p.stock_qty"),
        F.col("p.brand"),
    )
)
