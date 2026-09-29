from pyspark.sql import functions as F

result = (
    df_order_items
    .filter((F.col("unit_price") > 210) & F.col("order_id").isin([21, 22, 23, 26]) & (F.col("quantity") <= 6))
    .withColumn("line_total", F.round((F.col("quantity") / F.col("unit_price")), 2))
    .withColumn("unit_price_filled", F.coalesce("unit_price", F.lit(0)))
)
