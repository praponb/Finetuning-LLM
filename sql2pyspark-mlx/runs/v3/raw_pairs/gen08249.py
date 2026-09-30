from pyspark.sql import functions as F

result = (
    df_orders
    .filter(F.col("customer_id").isin([1, 30, 39]) & F.col("store_id").isNotNull() & F.col("discount").between(0.17, 0.38))
    .groupBy("status", F.year("order_date").alias("order_year"))
    .agg(F.count(F.when(F.col("channel") == "mobile", 1)).alias("mobile_count"))
    .orderBy("mobile_count")
)
