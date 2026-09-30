from pyspark.sql import functions as F

result = (
    df_order_items
    .filter(F.col("order_id").isin([13, 29]) & F.col("product_id").isNull())
    .groupBy("order_id")
    .agg(F.min("unit_price").alias("min_unit_price"))
    .orderBy(F.col("order_id").desc())
    .limit(5)
)
