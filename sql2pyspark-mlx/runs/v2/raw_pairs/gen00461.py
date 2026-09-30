from pyspark.sql import functions as F

result = (
    df_order_items
    .filter((F.col("order_id") == 28) & (F.col("quantity") >= 6) & (F.col("unit_price") > 130))
    .withColumn("unit_price_int", F.col("unit_price").cast("int"))
    .withColumn("line_total", (F.col("quantity") * F.col("unit_price")))
)
