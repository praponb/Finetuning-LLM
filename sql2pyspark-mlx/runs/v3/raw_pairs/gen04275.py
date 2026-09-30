from pyspark.sql import functions as F

result = (
    df_orders
    .groupBy(F.month("order_date").alias("order_month"))
    .agg(
        F.sum(F.when(F.col("status") == "COMPLETED", 1).otherwise(0)).alias("completed_count"),
        F.sort_array(F.collect_set("channel")).alias("channel_list"),
    )
    .filter(F.col("completed_count") < 39)
    .orderBy(F.col("completed_count").desc())
)
