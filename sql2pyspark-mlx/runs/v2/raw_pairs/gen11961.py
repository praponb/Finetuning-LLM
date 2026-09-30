from pyspark.sql import functions as F

result = (
    df_order_items
    .filter(F.col("product_id").isin([26, 35]))
    .withColumn("line_total", F.round((F.col("quantity") / F.col("unit_price")), 2))
    .orderBy(F.col("product_id").desc(), F.col("line_total").desc())
)
