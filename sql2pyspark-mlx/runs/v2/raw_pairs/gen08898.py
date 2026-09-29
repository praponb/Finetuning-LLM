from pyspark.sql import functions as F

product_summary = (
    df_order_items
    .groupBy("product_id")
    .agg(
        F.avg("quantity").alias("avg_quantity"),
        F.count("quantity").alias("quantity_count"),
    )
)

result = (
    df_products.alias("p")
    .join(product_summary.alias("P"), F.col("p.id") == F.col("P.product_id"), "left")
    .select(
        F.col("p.stock_qty"),
        F.col("p.launch_date"),
        F.col("P.avg_quantity"),
        F.col("P.quantity_count"),
    )
)
