from pyspark.sql import functions as F

exploded = (
    df_app_logs
    .select(
        "id",
        "host",
        "log_ts",
        "level",
        "status_code",
        F.explode("context").alias("attr_key", "attr_value"),
    )
)

result = (
    exploded
    .withColumn("activity_day", F.date_trunc("DAY", "log_ts"))
    .cube("host", "activity_day", "level")
    .agg(
        F.max("log_ts").alias("ended_at"),
        F.round(F.avg("status_code"), 2).alias("avg_status_code"),
        F.count(F.when(F.col("level") == "WARN", 1)).alias("warn_count"),
        F.grouping_id().alias("grouping_id"),
    )
    .filter(F.col("avg_status_code") > 0)
    .orderBy(F.col("host").desc())
)
