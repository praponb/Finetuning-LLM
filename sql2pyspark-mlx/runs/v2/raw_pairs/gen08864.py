from pyspark.sql import functions as F

result = (
    df_orders
    .filter(F.col("channel").isin(["store", "mobile"]) & F.col("customer_id").isin([12, 27, 34, 40]))
    .groupBy("status", F.quarter("order_date").alias("order_quarter"))
    .agg(
        F.max("discount").alias("max_discount"),
        F.count("*").alias("cnt"),
        F.round(F.stddev("amount"), 2).alias("stddev_amount"),
    )
    .filter(F.col("stddev_amount") >= 38)
    .orderBy("order_quarter", "status")
    .limit(5)
)
