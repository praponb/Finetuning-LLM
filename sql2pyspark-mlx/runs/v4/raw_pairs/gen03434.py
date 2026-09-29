from pyspark.sql import functions as F

base = (
    df_order_items
    .filter(F.col("quantity") <= 6)
    .withColumn("unit_price_whole", F.ceil("unit_price"))
    .withColumn("quantity_tier", F.when(F.col("quantity") >= 6, "High").when(F.col("quantity") >= 4, "Medium").otherwise("Low"))
)

result = (
    base
    .groupBy("product_id")
    .agg(
        F.countDistinct("order_id").alias("distinct_order_ids"),
        F.count("*").alias("cnt"),
    )
    .filter(F.col("cnt") >= 1)
    .orderBy("product_id")
)
