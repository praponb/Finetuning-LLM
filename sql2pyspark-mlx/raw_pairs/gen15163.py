from pyspark.sql import functions as F

result = (
    df_sessions
    .groupBy(F.date_trunc("MONTH", "started_at").alias("activity_month"))
    .agg(
        ((F.unix_timestamp(F.max("started_at")) - F.unix_timestamp(F.min("started_at"))) / 60).alias("duration_min"),
        F.count(F.when(F.col("country") == "UK", 1)).alias("uk_count"),
        F.count_if(F.col("device") == "tablet").alias("tablet_events"),
        F.count("*").alias("cnt"),
        F.count(F.when(F.col("country") == "JP", 1)).alias("jp_count"),
        F.size(F.collect_set("device")).alias("num_devices"),
    )
)
