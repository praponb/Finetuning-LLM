from pyspark.sql import functions as F

result = (
    df_products.alias("p")
    .join(df_order_items.filter(F.col("quantity") == 6).alias("oi"), F.col("p.id") == F.col("oi.product_id"), "left_semi")
    .filter(F.col("p.stock_qty") != 102)
    .select(F.col("p.id"), F.col("p.cost"), F.col("p.launch_date"), F.col("p.category"))
    .orderBy("launch_date")
)
