from pyspark.sql import functions as F

result = (
    df_orders
    .filter(F.col("shipping_fee") >= 39)
    .groupBy(
        F.year("order_date").alias("order_year"),
        F.date_format("order_date", "yyyy-MM").alias("order_month"),
    )
    .agg(
        F.round(F.stddev("discount"), 2).alias("stddev_discount"),
        F.count("*").alias("total_count"),
        F.round(F.avg("amount"), 2).alias("avg_amount"),
    )
)
