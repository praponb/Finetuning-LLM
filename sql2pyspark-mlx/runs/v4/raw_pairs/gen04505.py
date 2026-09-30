from pyspark.sql import functions as F

result = (
    df_order_items
    .filter(F.col("quantity").isNotNull() | (F.col("unit_price") <= 310))
    .withColumn("line_total", (F.col("unit_price") - F.col("quantity")))
)
