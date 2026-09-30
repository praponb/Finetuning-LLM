from pyspark.sql import functions as F

exploded = (
    df_app_logs
    .select(
        "id",
        "host",
        "log_ts",
        "level",
        "latency_ms",
        "status_code",
        F.explode("context").alias("key", "attr_value"),
    )
)

result = (
    exploded
    .withColumn("activity_day", F.date_trunc("DAY", "log_ts"))
    .groupingSets([["key", "activity_day"], ["key"]], "key", "activity_day")
    .agg(
        F.bit_and("status_code").alias("status_code_bit_and"),
        F.percentile_approx("latency_ms", [0.25, 0.5, 0.75]).alias("latency_ms_quartiles"),
        F.grouping_id().alias("gid"),
    )
    .orderBy("key")
)
