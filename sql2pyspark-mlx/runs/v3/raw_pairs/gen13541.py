from pyspark.sql import functions as F

result = (
    df_order_items
    .groupBy("product_id")
    .agg(
        F.count("quantity").alias("quantity_count"),
        (F.max("quantity") - F.min("quantity")).alias("quantity_range"),
        F.round(F.avg("quantity"), 2).alias("avg_quantity"),
    )
    .orderBy(F.col("product_id").desc())
    .limit(5)
)
