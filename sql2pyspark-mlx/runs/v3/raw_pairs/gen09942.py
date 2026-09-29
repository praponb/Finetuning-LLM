from pyspark.sql import functions as F

result = (
    df_orders
    .filter(F.col("channel").isNotNull())
    .groupBy(F.date_trunc("MONTH", "order_date").alias("order_month"), "status")
    .agg(
        F.round(F.stddev("discount"), 2).alias("stddev_discount"),
        F.sum("discount").alias("total_discount"),
    )
    .filter(F.col("total_discount") > 1.2)
    .orderBy("status", "order_month")
    .limit(10)
)
