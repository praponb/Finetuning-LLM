from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("key", "level").orderBy(F.col("log_ts").desc(), "id", "key")
w2 = Window.partitionBy("key").orderBy("log_ts", "id", "key")
w3 = Window.partitionBy("key").orderBy("log_ts", "id", "key").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w4 = Window.partitionBy("level", "host").orderBy("session_id", "id", "key")
w5 = Window.orderBy(F.col("is_new_session").asc_nulls_first())

lines = (
    df_app_logs
    .select(
        "id",
        "host",
        "log_ts",
        "level",
        "status_code",
        F.explode("context").alias("key", "v"),
    )
)

latest = (
    lines
    .withColumn("dedup_rank", F.row_number().over(w))
    .filter(F.col("dedup_rank") == 1)
    .drop("dedup_rank")
)

valid = (
    latest
    .filter((F.col("log_ts") >= F.lit("2025-06-02 00:00:00").cast("timestamp")) & F.col("key").isNotNull() & F.col("key").isin(["trace", "region"]))
    .select(
        "id",
        "host",
        "log_ts",
        "level",
        "key",
        F.substring("level", 1, 3).alias("level_prefix"),
        F.concat("key", F.lit("/"), "level").alias("key_level"),
    )
)

with_gaps = (
    valid
    .withColumn("is_new_session", F.when(F.lag("log_ts").over(w2).isNull() | (F.col("log_ts") > F.lag("log_ts").over(w2) + F.expr("INTERVAL 1 HOUR")), 1).otherwise(0))
)

islands = with_gaps.withColumn("session_id", F.sum("is_new_session").over(w3))

with_windows = (
    islands
    .withColumn("next_is_new_session", F.lead("is_new_session", 2).over(w4))
    .withColumn("is_new_session_cume_dist", F.cume_dist().over(w5))
)

totals = (
    with_windows
    .groupBy("session_id")
    .agg(
        F.sum(F.when(F.col("key") == "trace", F.col("is_new_session")).otherwise(0)).alias("trace_is_new_session"),
        ((F.unix_timestamp(F.max("log_ts")) - F.unix_timestamp(F.min("log_ts"))) / 60).alias("duration_min"),
        F.sort_array(F.collect_set("key")).alias("key_list"),
        F.percentile_approx("next_is_new_session", 0.9).alias("next_is_new_session_p90"),
        F.count("*").alias("cnt"),
    )
)

result = (
    totals
    .select(
        "session_id",
        "trace_is_new_session",
        "duration_min",
        "key_list",
        "next_is_new_session_p90",
        "cnt",
    )
    .orderBy(F.col("duration_min").desc_nulls_first())
)
