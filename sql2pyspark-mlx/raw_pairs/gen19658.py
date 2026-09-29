from pyspark.sql import functions as F

result = (
    df_orders
    .withColumn("activity_month", F.date_trunc("MONTH", "order_date"))
    .rollup("activity_month", "store_id", "customer_id")
    .agg(
        F.count_if(F.col("status") == "CANCELLED").alias("cancelled_events"),
        F.size(F.collect_set("channel")).alias("num_channels"),
        F.count("*").alias("num_rows"),
        F.grouping("activity_month").alias("grouping_id"),
    )
    .orderBy(F.col("store_id").asc_nulls_first())
)
