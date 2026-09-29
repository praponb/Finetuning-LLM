from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("hour_window").orderBy(F.col("search_count").desc())

parsed = (
    df_app_logs
    .filter(F.col("status_code") == 367)
    .select(
        "id",
        "service",
        "log_ts",
        "status_code",
        F.array_contains(F.map_keys("context"), "trace").alias("has_trace"),
        F.get_json_object("payload_json", "$.user.id").alias("user_id"),
        F.coalesce(F.col("context")["version"], F.lit("direct")).alias("version_or_default"),
        F.col("context")["trace"].alias("trace"),
        F.from_json("payload_json", "STRUCT<user: STRUCT<id: INT, tier: STRING>, duration: DOUBLE, retries: INT, tags: ARRAY<STRING>>")["retries"].alias("retry_count"),
    )
)

result = (
    parsed
    .groupBy(F.date_trunc("HOUR", "log_ts").alias("hour_window"), "service")
    .agg(
        F.count(F.when(F.col("service") == "search", 1)).alias("search_count"),
        ((F.unix_timestamp(F.max("log_ts")) - F.unix_timestamp(F.min("log_ts"))) / 60).alias("duration_min"),
        F.max("log_ts").alias("last_seen"),
    )
    .withColumn("search_count_rank", F.rank().over(w))
    .orderBy(F.col("search_count").desc_nulls_first())
)
