from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.orderBy(F.col("log_ts").desc(), "id", "key")
w2 = Window.orderBy("log_ts", "id", "key")
w3 = Window.partitionBy("host").orderBy("log_ts", "id", "key").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w4 = Window.partitionBy("key").orderBy("event_seq", "id", "key")
w5 = Window.partitionBy("key", "service")
w6 = Window.partitionBy("key").orderBy("log_ts", "id", "key")
w7 = Window.partitionBy("key").orderBy("log_ts", "id", "key").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w8 = Window.partitionBy("host").orderBy("log_ts", "id", "key")
w9 = Window.partitionBy("service", "key").orderBy(F.col("session_start_flag_2").desc_nulls_first(), "id", "key")

lines = (
    df_app_logs
    .select(
        "id",
        "service",
        "host",
        "log_ts",
        F.explode("context").alias("key", "attr_value"),
    )
)

enriched = (
    lines
    .select(
        "*",
        F.row_number().over(w).alias("row_seq"),
        F.row_number().over(w2).alias("event_seq"),
        F.last("key", ignorenulls=True).over(w3).alias("last_known_key"),
    )
)

cleaned = (
    enriched
    .filter((F.col("log_ts") >= F.lit("2025-06-01 07:00:00").cast("timestamp")) & F.col("key").isNotNull() & F.hour("log_ts").between(12, 16))
    .select(
        "id",
        "service",
        "host",
        "log_ts",
        "key",
        "event_seq",
        "last_known_key",
        F.substring("service", 1, 3).alias("service_prefix"),
    )
)

scored = (
    cleaned
    .withColumn("seq_no", F.row_number().over(w4))
    .withColumn("event_seq_share", F.round((F.col("event_seq") / F.sum("event_seq").over(w5)), 4))
)

with_gaps = (
    scored
    .withColumn("session_start_flag", F.when(F.lag("log_ts").over(w6).isNull() | (F.col("log_ts") > F.lag("log_ts").over(w6) + F.expr("INTERVAL 45 MINUTES")), 1).otherwise(0))
)

islands = with_gaps.withColumn("burst_id", F.sum("session_start_flag").over(w7))

flagged = (
    islands
    .withColumn("session_start_flag_2", F.when(F.lag("log_ts").over(w8).isNull() | (F.col("log_ts") > F.lag("log_ts").over(w8) + F.expr("INTERVAL 20 MINUTES")), 1).otherwise(0))
)

sessions_app_logs = flagged.withColumn("burst_id_2", F.sum("session_start_flag_2").over(w3))

latest = (
    sessions_app_logs
    .withColumn("rank_in_group", F.row_number().over(w9))
    .filter(F.col("rank_in_group") == 1)
)

result = (
    latest
    .select(
        "id",
        "service",
        "host",
        "log_ts",
        "key",
        "event_seq",
        "service_prefix",
        "event_seq_share",
        "burst_id",
    )
    .orderBy("service", "key")
)
