from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("attr_key").orderBy("status_code", "id", "attr_key").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w2 = Window.partitionBy("attr_key").orderBy("status_code", "id", "attr_key")
w3 = Window.partitionBy("attr_key").orderBy("latency_ms", "id", "attr_key").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_part = Window.partitionBy("attr_key").orderBy("next_latency_ms", "id", "attr_key").rowsBetween(-6, Window.currentRow)
w5 = Window.partitionBy("attr_key")
w6 = Window.partitionBy("attr_key").orderBy("last7_count_next_latency_ms", "id", "attr_key")
w_ordered = Window.orderBy("last_known_latency_ms", "id", "attr_key").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_hist = Window.partitionBy(F.date_trunc("YEAR", "log_ts"), "attr_key").orderBy(F.col("last7_avg_next_latency_ms").desc())
w_running = Window.partitionBy("attr_key").orderBy(F.col("log_ts").cast("bigint")).rangeBetween(-3600, Window.currentRow)
w10 = Window.partitionBy("attr_key").orderBy("log_ts", "id", "attr_key")
w11 = Window.partitionBy("attr_key").orderBy("log_ts", "id", "attr_key").rowsBetween(Window.unboundedPreceding, Window.currentRow)

lines = (
    df_app_logs
    .select(
        "id",
        "log_ts",
        "latency_ms",
        "status_code",
        F.explode("context").alias("attr_key", "value"),
    )
)

enriched = (
    lines
    .select(
        "id",
        "log_ts",
        "latency_ms",
        "attr_key",
        "value",
        F.last("latency_ms", ignorenulls=True).over(w).alias("last_known_latency_ms"),
        F.lead("latency_ms", 1).over(w2).alias("next_latency_ms"),
        F.first("attr_key", ignorenulls=True).over(w3).alias("first_known_attr_key"),
    )
)

enriched_app_logs = (
    enriched
    .select(
        "id",
        "log_ts",
        "latency_ms",
        "attr_key",
        "value",
        "last_known_latency_ms",
        "next_latency_ms",
        "first_known_attr_key",
        F.max("next_latency_ms").over(w_part).alias("last7_max_next_latency_ms"),
        F.count("next_latency_ms").over(w_part).alias("last7_count_next_latency_ms"),
        F.avg("next_latency_ms").over(w_part).alias("last7_avg_next_latency_ms"),
        F.round((F.col("next_latency_ms") / F.sum("next_latency_ms").over(w5)), 4).alias("next_latency_ms_share"),
    )
)

enriched_app_logs_2 = (
    enriched_app_logs
    .withColumn("next_last7_count_next_latency_ms", F.lead("last7_count_next_latency_ms", 1).over(w6))
    .withColumn("last_known_last7_max_next_latency_ms", F.last("last7_max_next_latency_ms", ignorenulls=True).over(w_ordered))
    .withColumn("last7_avg_next_latency_ms_percent_rank", F.percent_rank().over(w_hist))
    .withColumn("last7_max_next_latency_ms_count_1h", F.count("last7_max_next_latency_ms").over(w_running))
)

cleaned = (
    enriched_app_logs_2
    .filter(F.col("next_latency_ms_share").between(0.51, 0.71))
    .select(
        "id",
        "log_ts",
        "attr_key",
        "last_known_latency_ms",
        "next_latency_ms",
        "first_known_attr_key",
        "last7_max_next_latency_ms",
        "last7_avg_next_latency_ms",
        "next_latency_ms_share",
        "next_last7_count_next_latency_ms",
        "last7_avg_next_latency_ms_percent_rank",
        "last7_max_next_latency_ms_count_1h",
        F.greatest("last_known_latency_ms", "last7_max_next_latency_ms_count_1h").alias("greatest_last_known_latency_ms_last7_max_next_latency_ms_count_1h"),
    )
)

filtered = (
    cleaned
    .filter(F.col("attr_key").isNotNull() & (~F.col("attr_key").eqNullSafe(F.lit("trace"))))
    .select(
        "id",
        "log_ts",
        "attr_key",
        "next_latency_ms",
        "first_known_attr_key",
        "last7_max_next_latency_ms",
        "last7_avg_next_latency_ms",
        "last7_avg_next_latency_ms_percent_rank",
        "greatest_last_known_latency_ms_last7_max_next_latency_ms_count_1h",
        F.initcap(F.trim("attr_key")).alias("attr_key_title"),
    )
)

with_gaps = (
    filtered
    .withColumn("new_group_flag", F.when(F.lag("log_ts").over(w10).isNull() | (F.col("log_ts") > F.lag("log_ts").over(w10) + F.expr("INTERVAL 15 MINUTES")), 1).otherwise(0))
)

islands = with_gaps.withColumn("session_no", F.sum("new_group_flag").over(w11))

result = (
    islands
    .select(
        "id",
        "log_ts",
        "attr_key",
        "first_known_attr_key",
        "last7_max_next_latency_ms",
        "greatest_last_known_latency_ms_last7_max_next_latency_ms_count_1h",
        "new_group_flag",
        "session_no",
    )
    .orderBy(
        F.col("greatest_last_known_latency_ms_last7_max_next_latency_ms_count_1h").asc_nulls_last(),
    )
)
