from pyspark.sql import functions as F

lines = (
    df_app_logs
    .select(
        "id",
        "service",
        "log_ts",
        "status_code",
        F.explode_outer("context").alias("attr_key", "v"),
    )
)

result = (
    lines
    .withColumn("activity_day", F.date_trunc("DAY", "log_ts"))
    .rollup("activity_day", "attr_key")
    .agg(
        F.countDistinct("service").alias("distinct_services"),
        F.round(F.avg("status_code"), 2).alias("avg_status_code"),
        ((F.unix_timestamp(F.max("log_ts")) - F.unix_timestamp(F.min("log_ts"))) / 60).alias("duration_min"),
        F.grouping("activity_day").alias("grouping_level"),
    )
    .orderBy("duration_min")
)
