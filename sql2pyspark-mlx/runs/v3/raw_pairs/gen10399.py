from pyspark.sql import functions as F

result = (
    df_orders
    .filter((F.year("order_date") >= 2025) & F.col("store_id").isNull() & (F.col("channel") == "partner"))
    .groupBy("channel")
    .agg(
        F.count("*").alias("total_count"),
        F.sort_array(F.collect_set("channel")).alias("channel_list"),
        F.countDistinct("amount").alias("unique_channels"),
    )
    .orderBy(F.col("unique_channels").desc(), "channel")
)
