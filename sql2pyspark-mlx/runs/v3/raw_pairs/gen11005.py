from pyspark.sql import functions as F

result = (
    df_order_items
    .filter(F.col("order_id").isin([11, 12, 30]))
    .groupBy("order_id")
    .agg(
        F.round(F.avg("quantity"), 2).alias("avg_quantity"),
        F.count("quantity").alias("quantity_count"),
    )
    .filter(F.col("quantity_count") < 5)
    .select("order_id", "avg_quantity", "quantity_count")
)
