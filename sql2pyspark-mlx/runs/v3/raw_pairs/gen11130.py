from pyspark.sql import functions as F

result = (
    df_order_items
    .filter(F.col("quantity").between(6, 7))
    .withColumn("unit_price_filled", F.coalesce("unit_price", F.lit(0)))
    .withColumn("unit_price_tier", F.when(F.col("unit_price") >= 400, "High").when(F.col("unit_price") >= 180, "Medium").otherwise("Low"))
)
