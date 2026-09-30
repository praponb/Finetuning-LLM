from pyspark.sql import functions as F

result = (
    df_order_items
    .withColumn("line_total", (F.col("unit_price") - F.col("quantity")))
    .withColumn("unit_price_adjusted", F.round((F.col("unit_price") * 1.07), 2))
)
