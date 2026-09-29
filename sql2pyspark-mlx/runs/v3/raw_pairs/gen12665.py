from pyspark.sql import functions as F

result = (
    df_order_items
    .filter(F.col("order_id").isin([9, 25]) & (F.col("quantity") < 7))
    .groupBy("product_id")
    .agg(F.round(F.stddev("unit_price"), 2).alias("stddev_unit_price"))
    .orderBy(F.col("product_id").desc())
)
