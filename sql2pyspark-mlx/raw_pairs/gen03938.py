from pyspark.sql import functions as F

exploded = (
    df_app_logs
    .select(
        "id",
        "service",
        "host",
        "log_ts",
        "status_code",
        F.explode("context").alias("k", "value"),
    )
)

result = (
    exploded
    .withColumn("activity_day", F.date_trunc("DAY", "log_ts"))
    .cube("activity_day")
    .agg(
        F.min("status_code").alias("min_status_code"),
        F.sum(F.when(F.col("host") == "h2", F.col("status_code")).otherwise(0)).alias("h2_status_code"),
        F.sort_array(F.collect_set("service")).alias("service_list"),
        F.count(F.when(F.col("host") == "h3", 1)).alias("h3_count"),
        F.count(F.when(F.col("service") == "search", 1)).alias("search_count"),
        F.grouping("activity_day").alias("grouping_id"),
    )
    .filter(F.col("min_status_code") > 0)
)
