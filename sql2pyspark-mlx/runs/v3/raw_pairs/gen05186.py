from pyspark.sql import functions as F

result = (
    df_order_items
    .withColumn("greatest_quantity_unit_price", F.greatest("quantity", "unit_price"))
    .withColumn("least_quantity_unit_price", F.least("quantity", "unit_price"))
    .orderBy("order_id", "quantity")
)
