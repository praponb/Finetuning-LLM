from pyspark.sql import functions as F

src = (
    df_orders
    .filter((F.col("customer_id") == 31) & (F.col("customer_id").isin([3, 20, 21]) | (F.col("shipping_fee") < 29)))
)

orders_stats = (
    src
    .groupBy("store_id", F.year("order_date").alias("order_year"))
    .agg(
        F.avg("discount").alias("avg_discount"),
        F.sort_array(F.collect_set("status")).alias("status_list"),
    )
)

result = (
    orders_stats
    .filter(F.col("avg_discount") >= 0.38)
    .orderBy("avg_discount", F.col("order_year").desc(), "store_id")
    .limit(20)
)
