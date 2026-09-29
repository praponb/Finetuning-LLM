from pyspark.sql import functions as F

result = (
    df_order_items
    .filter(F.col("quantity").isNull())
    .withColumn("unit_price_rounded", F.round("unit_price", 0))
)
