from pyspark.sql import functions as F

result = (
    df_orders
    .filter(~(F.col("order_date").between("2024-02-01", "2024-12-01") & F.col("channel").isin(["store", "mobile"]) & (F.col("status") == "CANCELLED")))
    .groupBy("status", "customer_id")
    .agg(
        F.sort_array(F.collect_set("channel")).alias("channel_list"),
        F.sum(F.when(F.col("status") == "COMPLETED", 1).otherwise(0)).alias("completed_count"),
    )
)
