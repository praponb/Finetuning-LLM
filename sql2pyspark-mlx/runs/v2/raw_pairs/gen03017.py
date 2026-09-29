from pyspark.sql import functions as F

result = (
    df_orders
    .filter(F.col("shipping_fee").between(26, 38) & (F.col("order_date") >= "2025-03-01"))
    .agg(F.round(F.stddev("discount"), 2).alias("stddev_discount"))
    .orderBy(F.col("stddev_discount").desc())
)
