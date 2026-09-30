from pyspark.sql import functions as F

valid_order_items = (
    df_order_items
    .filter((F.col("quantity") == 4) & F.col("order_id").isin([7, 18, 19, 40]))
    .withColumn("unit_price_adjusted", F.round((F.col("unit_price") * 1.15), 2))
)

product_id_summary = (
    valid_order_items
    .groupBy("product_id")
    .agg(F.countDistinct("quantity").alias("unique_orders"))
)

result = (
    product_id_summary
    .filter(F.col("unique_orders") >= 9)
    .orderBy(F.col("unique_orders").desc())
)
