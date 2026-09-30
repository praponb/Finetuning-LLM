from pyspark.sql import functions as F

result = (
    df_orders
    .filter(F.col("customer_id").isNull())
    .groupBy(F.date_format("order_date", "yyyy-MM").alias("order_month"), "status")
    .agg(
        F.count(F.when(F.col("status") == "CANCELLED", 1)).alias("cancelled_count"),
        F.round(F.stddev("discount"), 2).alias("stddev_discount"),
        F.round(F.avg("amount"), 2).alias("avg_amount"),
    )
    .filter(F.col("cancelled_count") > 2)
)
