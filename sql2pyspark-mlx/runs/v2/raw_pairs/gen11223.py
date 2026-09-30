from pyspark.sql import functions as F

result = (
    df_order_items.alias("order_items")
    .join(df_products.alias("products"), F.col("order_items.product_id") == F.col("products.id"), "inner")
    .groupBy(
        F.year(F.col("products.launch_date")).alias("launch_year"),
        F.date_format(F.col("products.launch_date"), "yyyy-MM").alias("launch_month"),
    )
    .agg(
        F.round(F.avg(F.col("order_items.quantity")), 2).alias("avg_quantity"),
        F.max(F.col("order_items.quantity")).alias("max_quantity"),
    )
    .filter(F.col("avg_quantity") >= 45)
    .orderBy("max_quantity", F.col("launch_month").desc())
)
