from pyspark.sql import functions as F

result = (
    df_order_items
    .filter(F.col("unit_price") <= 400)
    .withColumn("least_unit_price_quantity", F.least("unit_price", "quantity"))
    .orderBy(F.col("id").desc(), F.col("product_id").desc())
)
