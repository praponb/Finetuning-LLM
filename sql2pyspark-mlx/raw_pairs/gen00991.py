from pyspark.sql import functions as F

result = (
    df_sessions
    .withColumn("week_start", F.date_trunc("WEEK", "started_at"))
    .rollup("week_start", "device")
    .agg(
        F.size(F.collect_set("device")).alias("num_devices"),
        F.count(F.when(F.col("country") == "SG", 1)).alias("sg_count"),
        ((F.unix_timestamp(F.max("started_at")) - F.unix_timestamp(F.min("started_at"))) / 60).alias("span_minutes"),
        F.grouping_id().alias("grouping_level"),
    )
    .filter(F.col("sg_count") > 1)
)
