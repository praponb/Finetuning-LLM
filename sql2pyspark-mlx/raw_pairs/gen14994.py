from pyspark.sql import functions as F

result = (
    df_products.alias("p")
    .join(df_order_items.filter(F.col("quantity") >= 4).alias("oi"), F.col("p.id") == F.col("oi.product_id"), "left_anti")
    .filter(F.col("p.stock_qty") == 121)
    .select(
        F.col("p.id"),
        F.col("p.cost"),
        F.col("p.stock_qty"),
        F.col("p.launch_date"),
    )
)
