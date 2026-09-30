from pyspark.sql import functions as F

result = (
    df_order_items
    .filter(F.col("order_id").isin([1, 19, 23, 30]))
    .withColumn("least_quantity_unit_price", F.least("quantity", "unit_price"))
)
