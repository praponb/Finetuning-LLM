from pyspark.sql import functions as F

result = (
    df_products.alias("p")
    .join(df_order_items.filter(F.col("product_id") == 38).alias("oi"), F.col("p.id") == F.col("oi.product_id"), "left_anti")
    .select(
        F.col("p.id"),
        F.col("p.supplier_id"),
        F.col("p.launch_date"),
        F.col("p.stock_qty"),
    )
    .orderBy("id")
)
