from pyspark.sql import functions as F

result = (
    df_order_items
    .filter(F.col("order_id") == 10)
    .withColumn("line_total", (F.col("quantity") - F.col("unit_price")))
    .withColumn("line_total_2", (F.col("quantity") * F.col("unit_price")))
    .orderBy("id")
)
