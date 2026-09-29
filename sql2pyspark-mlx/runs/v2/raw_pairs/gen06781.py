from pyspark.sql import functions as F

filtered_order_items = df_order_items.filter(F.col("quantity") < 6)

agg = (
    filtered_order_items
    .groupBy("product_id")
    .agg(
        F.round(F.avg("quantity"), 2).alias("avg_quantity"),
        F.count("quantity").alias("quantity_count"),
    )
)

result = agg
