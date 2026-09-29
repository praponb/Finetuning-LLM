from pyspark.sql import functions as F

exploded = (
    df_app_logs
    .select(
        "id",
        "service",
        "host",
        "log_ts",
        F.explode_outer("context").alias("k", "attr_value"),
    )
)

result = (
    exploded
    .withColumn("activity_day", F.date_trunc("DAY", "log_ts"))
    .rollup("activity_day", "service")
    .agg(
        F.count("*").alias("row_count"),
        F.count_if(F.col("k") == "region").alias("region_events"),
        F.grouping_id().alias("grouping_level"),
    )
    .filter(F.col("region_events") >= 1)
)
