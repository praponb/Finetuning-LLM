from pyspark.sql import functions as F

result = (
    df_order_items
    .filter(F.col("quantity").isNotNull())
    .withColumn("line_total", (F.col("quantity") - F.col("unit_price")))
    .withColumn("line_total_2", (F.col("unit_price") - F.col("quantity")))
    .orderBy(F.col("line_total_2").desc(), "product_id", "id")
    .limit(3)
)
