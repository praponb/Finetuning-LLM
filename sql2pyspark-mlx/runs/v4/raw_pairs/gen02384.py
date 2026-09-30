from pyspark.sql import functions as F

valid_orders = (
    df_orders
    .filter((F.col("status") == "SHIPPED") & (F.col("shipping_fee") <= 10) & (F.col("amount") > 510))
)

channel_summary = (
    valid_orders
    .groupBy("channel", "status")
    .agg(
        F.sort_array(F.collect_set("status")).alias("status_list"),
        F.sum(F.when(F.col("status") == "PENDING", 1).otherwise(0)).alias("pending_count"),
    )
)

result = (
    channel_summary
    .filter(F.col("pending_count") >= 59)
    .select("channel", "status_list", "pending_count")
    .orderBy(F.col("pending_count").desc(), "channel")
)
