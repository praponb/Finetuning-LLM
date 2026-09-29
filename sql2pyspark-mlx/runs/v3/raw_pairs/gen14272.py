from pyspark.sql import functions as F

result = (
    df_orders
    .filter(~(F.month("order_date").isin([8, 10, 12]) & F.col("store_id").isin([3, 7, 14, 29]) & (F.year("order_date") >= 2023)))
    .groupBy("channel")
    .agg(F.round(F.stddev("amount"), 2).alias("stddev_amount"))
)
