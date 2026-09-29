from pyspark.sql import functions as F

result = (
    df_order_items
    .filter(F.col("order_id").isin([25, 40]))
    .groupBy(
        F.when(F.col("quantity") >= 7, "high").when(F.col("quantity") >= 4, "mid").otherwise("low").alias("quantity_bucket"),
    )
    .agg(
        F.count("unit_price").alias("unit_price_count"),
        F.round(F.avg("quantity"), 2).alias("avg_quantity"),
        F.avg("unit_price").alias("avg_unit_price"),
    )
    .filter(F.col("avg_unit_price") < 340)
    .orderBy("quantity_bucket")
)
