from pyspark.sql import functions as F

exploded = df_app_logs.select("id", "host", "log_ts", F.explode("context").alias("key", "v"))

result = (
    exploded
    .groupBy(F.date_trunc("HOUR", "log_ts").alias("hour_window"))
    .agg(
        F.countDistinct("key").alias("distinct_keys"),
        F.count_if(F.col("key") == "trace").alias("trace_events"),
        ((F.unix_timestamp(F.max("log_ts")) - F.unix_timestamp(F.min("log_ts"))) / 60).alias("span_minutes"),
    )
    .filter(F.col("span_minutes") > 0)
)
