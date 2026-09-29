from pyspark.sql import functions as F

cleaned = (
    df_order_items
    .filter(F.col("unit_price") <= 150)
    .withColumn("unit_price_band", F.when(F.col("unit_price") >= 320, "High").when(F.col("unit_price") >= 70, "Medium").otherwise("Low"))
    .withColumn("line_total", (F.col("unit_price") - F.col("quantity")))
)

result = (
    cleaned
    .groupBy("product_id")
    .agg(
        F.countDistinct("order_id").alias("distinct_order_ids"),
        F.avg("unit_price").alias("avg_unit_price"),
    )
    .orderBy(F.col("avg_unit_price").desc())
)
