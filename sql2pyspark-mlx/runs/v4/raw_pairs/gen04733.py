from pyspark.sql import functions as F

result = (
    df_order_items
    .filter(F.col("quantity").isNotNull())
    .withColumn("line_total", F.round((F.col("quantity") / F.col("unit_price")), 2))
    .withColumn("unit_price_rounded", F.round("unit_price", 2))
    .orderBy("unit_price")
)
