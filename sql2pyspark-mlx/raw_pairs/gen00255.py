from pyspark.sql import functions as F

result = (
    df_order_items
    .filter(F.col("unit_price").between(190, 350) | (F.col("quantity") <= 7))
    .groupBy("order_id")
    .agg(F.round(F.stddev("quantity"), 2).alias("stddev_quantity"))
)
