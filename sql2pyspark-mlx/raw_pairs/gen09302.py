from pyspark.sql import functions as F

result = (
    df_order_items
    .filter(F.col("product_id").isNull() & (F.col("unit_price") >= 310))
    .withColumn("quantity_num", F.col("quantity").cast("double"))
    .orderBy(F.col("order_id").desc(), "id")
    .limit(3)
)
