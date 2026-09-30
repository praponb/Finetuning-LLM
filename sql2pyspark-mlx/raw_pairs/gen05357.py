from pyspark.sql import functions as F

exploded = (
    df_app_logs
    .select("id", "log_ts", F.explode("context").alias("attr_key", "attr_value"))
)

result = (
    exploded
    .groupBy("attr_key")
    .agg(
        F.max("log_ts").alias("last_seen"),
        F.sort_array(F.collect_set("attr_key")).alias("attr_key_list"),
        F.max("log_ts").alias("last_seen_2"),
    )
    .orderBy(F.col("attr_key").asc_nulls_last())
)
