from pyspark.sql import functions as F

result = (
    df_sessions
    .groupBy("device")
    .agg(
        F.sort_array(F.collect_set("device")).alias("device_list"),
        F.count_if(F.col("device") == "tablet").alias("tablet_events"),
        F.count(F.when(F.col("country") == "JP", 1)).alias("jp_count"),
        F.countDistinct("customer_id").alias("distinct_customer_ids"),
    )
)
