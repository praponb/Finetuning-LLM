from pyspark.sql import functions as F

result = (
    df_order_items
    .filter(F.col("quantity") != 5)
    .agg(F.min("unit_price").alias("min_unit_price"))
)
