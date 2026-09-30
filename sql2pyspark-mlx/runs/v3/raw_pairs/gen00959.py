from pyspark.sql import functions as F

result = (
    df_order_items
    .filter(F.col("order_id").isin([20, 31]) & (F.col("product_id") == 10) & F.col("unit_price").between(150, 260))
    .groupBy(
        F.when(F.col("unit_price") >= 350, "high").when(F.col("unit_price") >= 140, "mid").otherwise("low").alias("unit_price_bucket"),
    )
    .agg(
        F.round(F.avg("quantity"), 2).alias("avg_quantity"),
        F.sum("quantity").alias("total_quantity"),
        (F.max("quantity") - F.min("quantity")).alias("quantity_range"),
    )
    .orderBy("unit_price_bucket", "quantity_range")
    .limit(3)
)
