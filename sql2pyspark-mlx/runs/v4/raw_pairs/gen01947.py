from pyspark.sql import functions as F

result = (
    df_order_items
    .withColumn("least_unit_price_quantity", F.least("unit_price", "quantity"))
    .withColumn("least_quantity_unit_price", F.least("quantity", "unit_price"))
    .orderBy(F.col("quantity").desc())
)
