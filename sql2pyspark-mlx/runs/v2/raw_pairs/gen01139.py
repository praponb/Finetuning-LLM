from pyspark.sql import functions as F

result = (
    df_order_items
    .filter(~(F.col("order_id").isin([8, 11, 35]) & (F.col("unit_price") <= 250) & F.col("product_id").isin([10, 16, 26, 28])))
    .groupBy("product_id")
    .agg(
        F.round(F.avg("unit_price"), 2).alias("avg_unit_price"),
        F.min("unit_price").alias("min_unit_price"),
    )
    .orderBy(F.col("min_unit_price").desc())
)
