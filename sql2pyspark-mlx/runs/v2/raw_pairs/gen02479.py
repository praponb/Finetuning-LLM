from pyspark.sql import functions as F

result = (
    df_order_items
    .filter((F.col("unit_price") >= 170) & F.col("product_id").isNull())
    .withColumn("line_total", F.round((F.col("quantity") / F.col("unit_price")), 2))
    .withColumn("adjusted_quantity", F.round((F.col("quantity") * 1.07), 2))
)
