from pyspark.sql import functions as F

result = (
    df_orders
    .groupBy("channel")
    .agg(
        F.round(F.stddev("discount"), 2).alias("stddev_discount"),
        F.min("order_date").alias("min_order_date"),
        F.avg("amount").alias("avg_amount"),
    )
    .filter(F.col("stddev_discount") < 52)
    .orderBy("avg_amount")
)
