from pyspark.sql import functions as F

result = (
    df_orders
    .filter((F.col("shipping_fee") < 30) & (F.col("status") == "SHIPPED"))
    .groupBy("channel")
    .agg(
        F.sort_array(F.collect_set("channel")).alias("channel_list"),
        F.count("*").alias("row_count"),
        F.sum(F.when(F.col("status") == "CANCELLED", 1).otherwise(0)).alias("cancelled_count"),
    )
    .filter(F.col("cancelled_count") >= 30)
    .orderBy("cancelled_count", "channel")
    .limit(20)
)
