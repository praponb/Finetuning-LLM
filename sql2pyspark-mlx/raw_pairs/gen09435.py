from pyspark.sql import functions as F

result = (
    df_order_items
    .filter(F.col("product_id").isNotNull() & F.col("product_id").isin([1, 2, 18, 24]))
    .withColumn("unit_price_scaled", F.round((F.col("unit_price") * 1.07), 2))
    .withColumn("unit_price_filled", F.coalesce("unit_price", F.lit(0)))
    .orderBy("order_id")
)
