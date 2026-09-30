from pyspark.sql import functions as F

result = (
    df_order_items
    .filter(F.col("quantity") >= 5)
    .groupBy("order_id")
    .agg(F.max("unit_price").alias("max_unit_price"))
    .orderBy(F.col("order_id").desc())
)
