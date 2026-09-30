from pyspark.sql import functions as F

result = (
    df_orders
    .groupBy("customer_id")
    .agg(F.round(F.stddev("amount"), 2).alias("stddev_amount"))
)
