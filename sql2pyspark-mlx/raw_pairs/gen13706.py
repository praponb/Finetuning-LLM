from pyspark.sql import functions as F

exploded = (
    df_app_logs
    .select(
        "id",
        "service",
        "host",
        "log_ts",
        F.explode_outer("context").alias("key", "v"),
    )
)

result = (
    exploded
    .withColumn("activity_day", F.date_trunc("DAY", "log_ts"))
    .groupingSets([["key", "activity_day"], ["key"]], "key", "activity_day")
    .agg(
        F.count(F.when(F.col("key") == "trace", 1)).alias("trace_rows"),
        ((F.unix_timestamp(F.max("log_ts")) - F.unix_timestamp(F.min("log_ts"))) / 60).alias("span_minutes"),
        F.size(F.collect_set("service")).alias("num_services"),
        F.count_if(F.col("service") == "billing").alias("billing_events"),
        F.countDistinct("key").alias("distinct_keys"),
        F.sort_array(F.collect_set("key")).alias("key_list"),
    )
    .filter(F.col("billing_events") >= 1)
    .orderBy(F.col("activity_day").desc())
)
