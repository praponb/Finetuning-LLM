from pyspark.sql import functions as F

result = (
    df_order_items
    .filter(F.col("product_id").isin([18, 31, 33, 36]))
    .withColumn("line_total", F.round((F.col("unit_price") / F.col("quantity")), 2))
    .withColumn("line_total_2", (F.col("quantity") + F.col("unit_price")))
    .orderBy(F.col("unit_price").desc(), "quantity")
)
