from pyspark.sql import functions as F

result = (
    df_order_items.alias("order_items")
    .join(df_products.alias("products"), F.col("order_items.product_id") == F.col("products.id"), "left")
    .groupBy(F.date_trunc("MONTH", F.col("products.launch_date")).alias("launch_month"))
    .agg(F.min(F.col("order_items.quantity")).alias("min_quantity"))
    .orderBy(F.col("launch_month").desc(), F.col("min_quantity").desc())
)
