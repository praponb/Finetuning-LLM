from pyspark.sql import functions as F

result = (
    df_order_items
    .filter(F.col("unit_price") <= 300)
    .withColumn("quantity_num", F.col("quantity").cast("string"))
    .orderBy(F.col("unit_price").desc(), "id")
    .limit(20)
)
