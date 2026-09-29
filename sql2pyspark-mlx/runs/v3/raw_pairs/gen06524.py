from pyspark.sql import functions as F

result = (
    df_order_items
    .filter(F.col("unit_price").isNotNull() & F.col("unit_price").between(160, 330) & (F.col("quantity") == 3))
    .withColumn("unit_price_int", F.col("unit_price").cast("int"))
)
