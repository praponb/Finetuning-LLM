from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("level").orderBy("log_ts", "id", "k")
w2 = Window.partitionBy("level").orderBy("log_ts", "id", "k").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w3 = Window.partitionBy("k", "level").orderBy(F.col("latency_ms_rounded").asc_nulls_first(), "id", "k")
w4 = Window.partitionBy("k").orderBy("session_no", "id", "k").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w5 = Window.partitionBy("k").orderBy("log_ts", "id", "k")
w6 = Window.partitionBy("k").orderBy("log_ts", "id", "k").rowsBetween(Window.unboundedPreceding, Window.currentRow)

exploded = (
    df_app_logs
    .select(
        "id",
        "log_ts",
        "level",
        "latency_ms",
        F.explode("context").alias("k", "value"),
    )
)

base = (
    exploded
    .filter((F.col("log_ts") > F.lit("2025-06-01").cast("date")) & F.col("k").isNotNull())
    .select("*", F.round("latency_ms", 1).alias("latency_ms_rounded"))
)

with_gaps = (
    base
    .withColumn("session_start_flag", F.when(F.col("log_ts") - F.lag("log_ts").over(w) > F.expr("INTERVAL 20 MINUTES"), 1).otherwise(0))
)

sessions_app_logs = (
    with_gaps
    .select(
        "id",
        "log_ts",
        "level",
        "latency_ms",
        "k",
        "value",
        "latency_ms_rounded",
        F.sum("session_start_flag").over(w2).alias("session_no"),
    )
)

ranked = (
    sessions_app_logs
    .withColumn("rank_in_group", F.row_number().over(w3))
    .filter(F.col("rank_in_group") == 1)
)

enriched = (
    ranked
    .select(
        "id",
        "log_ts",
        "k",
        "rank_in_group",
        F.count("latency_ms_rounded").over(w4).alias("running_count_latency_ms_rounded"),
        F.avg("latency_ms_rounded").over(w4).alias("running_avg_latency_ms_rounded"),
        F.max("latency_ms_rounded").over(w4).alias("running_max_latency_ms_rounded"),
    )
)

with_gaps_2 = (
    enriched
    .withColumn("session_start_flag", F.when(F.col("log_ts") - F.lag("log_ts").over(w5) > F.expr("INTERVAL 30 MINUTES"), 1).otherwise(0))
)

sessions_app_logs_2 = with_gaps_2.withColumn("session_no", F.sum("session_start_flag").over(w6))

result = (
    sessions_app_logs_2
    .select(
        "id",
        "k",
        "rank_in_group",
        "running_count_latency_ms_rounded",
        "running_avg_latency_ms_rounded",
        "running_max_latency_ms_rounded",
        F.when(F.col("k").rlike("^(trace|version)$"), "CORE").when(F.col("k").rlike("^(region)$"), "GROUP_B").otherwise("EXTENDED").alias("k_zone"),
    )
    .orderBy(F.col("id").desc(), "running_avg_latency_ms_rounded")
)
