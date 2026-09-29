from pyspark.sql import functions as F

result = (
    df_order_items
    .filter(F.col("unit_price").isNotNull() & ((F.col("unit_price") < 240) | F.col("product_id").isin([11, 28])))
    .withColumn("unit_price_tier", F.when(F.col("unit_price") >= 310, "High").when(F.col("unit_price") >= 130, "Medium").otherwise("Low"))
    .withColumn("line_total", (F.col("unit_price") * F.col("quantity")))
)
