from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("host").orderBy("log_ts", "id", "k")
w2 = Window.partitionBy("host").orderBy("log_ts", "id", "k").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w3 = Window.partitionBy("service").orderBy(F.col("log_ts").cast("bigint")).rangeBetween(-3600, Window.currentRow)
w4 = Window.partitionBy("k")
w5 = Window.partitionBy("service", "k").orderBy(F.col("log_ts").cast("bigint")).rangeBetween(-900, Window.currentRow)
w6 = Window.partitionBy("k").orderBy(F.col("log_ts").cast("bigint")).rangeBetween(-7200, Window.currentRow)

flattened = (
    df_app_logs
    .select(
        "id",
        "service",
        "host",
        "log_ts",
        "level",
        "latency_ms",
        "status_code",
        F.explode("context").alias("k", "value"),
    )
)

valid = (
    flattened
    .filter(F.col("k").isNotNull())
    .withColumn("is_warn", F.when(F.col("level") == "WARN", 1).otherwise(0))
)

prev_events = valid.withColumn("prev_log_ts", F.lag("log_ts").over(w))

with_gaps = (
    prev_events
    .withColumn("is_new_burst", F.when(F.col("prev_log_ts").isNull() | (F.col("log_ts") > F.col("prev_log_ts") + F.expr("INTERVAL 15 MINUTES")), 1).otherwise(0))
)

sessions_app_logs = (
    with_gaps
    .select(
        "id",
        "service",
        "host",
        "log_ts",
        "latency_ms",
        "status_code",
        "k",
        "value",
        "is_warn",
        F.sum("is_new_burst").over(w2).alias("session_id"),
    )
)

kept_keys = (
    sessions_app_logs
    .select("service", "id")
    .intersectAll(
        df_app_logs
        .filter(F.hour("log_ts").between(11, 15))
        .select("service", "id")
    )
)

verified = (
    sessions_app_logs
    .alias("s")
    .join(kept_keys.alias("k"), (F.col("s.service") == F.col("k.service")) & (F.col("s.id") == F.col("k.id")), "inner")
    .select("s.*")
)

metrics = (
    verified
    .withColumn("status_code_avg_1h", F.avg("status_code").over(w3))
    .withColumn("corr_status_code_latency_ms", F.corr("status_code", "latency_ms").over(w4))
    .withColumn("status_code_avg_15m", F.avg("status_code").over(w5))
    .withColumn("is_warn_sum_2h", F.sum("is_warn").over(w6))
)

totals = (
    metrics
    .groupBy("k")
    .agg(
        F.sum("corr_status_code_latency_ms").alias("total_corr_status_code_latency_ms"),
        F.count(F.when(F.col("service") == "auth", 1)).alias("auth_count"),
        F.percentile_approx("session_id", [0.25, 0.5, 0.75]).alias("session_id_quartiles"),
    )
    .filter(F.col("total_corr_status_code_latency_ms") < 1000000)
)

result = (
    totals
    .select("k", "auth_count", "session_id_quartiles")
    .orderBy("auth_count")
)
