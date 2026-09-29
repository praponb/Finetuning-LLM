from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("level").orderBy(F.col("log_ts").cast("bigint")).rangeBetween(-7200, Window.currentRow)
w2 = Window.orderBy(F.col("latency_ms_sum_2h").asc_nulls_first())
w_grp = Window.partitionBy("key").orderBy(F.col("log_ts").cast("bigint")).rangeBetween(-7200, Window.currentRow)

exploded = (
    df_app_logs
    .select(
        "id",
        "log_ts",
        "level",
        "latency_ms",
        F.explode("context").alias("key", "attr_value"),
    )
)

kept_keys = (
    exploded
    .select("level")
    .intersectAll(
        df_app_logs
        .filter(F.col("host") == "h3")
        .select("level")
    )
)

reconciled = (
    exploded
    .alias("s")
    .join(kept_keys.alias("k"), (F.col("s.level") == F.col("k.level")), "inner")
    .select("s.*")
)

metrics = (
    reconciled
    .filter(F.col("level").isin(["WARN", "ERROR"]))
    .select(
        "id",
        "log_ts",
        "key",
        F.sum("latency_ms").over(w).alias("latency_ms_sum_2h"),
    )
)

enriched = (
    metrics
    .select(
        "id",
        "log_ts",
        F.dense_rank().over(w2).alias("latency_ms_sum_2h_dense_rank"),
        F.count("latency_ms_sum_2h").over(w_grp).alias("latency_ms_sum_2h_count_2h"),
    )
)

valid = (
    enriched
    .filter(F.col("log_ts") >= F.lit("2025-06-01 05:00:00").cast("timestamp"))
    .select(
        "*",
        F.round("latency_ms_sum_2h_dense_rank", 2).alias("latency_ms_sum_2h_dense_rank_rounded"),
    )
)

valid_app_logs = (
    valid
    .filter((F.col("log_ts") > F.lit("2025-06-01").cast("date")) & (F.col("latency_ms_sum_2h_dense_rank_rounded") <= 29))
    .withColumn("latency_ms_sum_2h_count_2h_per_latency_ms_sum_2h_dense_rank_rounded", (F.col("latency_ms_sum_2h_count_2h") / F.nullif("latency_ms_sum_2h_dense_rank_rounded", F.lit(0))))
)

summary = (
    valid_app_logs
    .groupBy(
        F.date_trunc("HOUR", "log_ts").alias("hour_window"),
        F.when(F.col("latency_ms_sum_2h_count_2h") >= 15, "HIGH").otherwise("LOW").alias("latency_ms_sum_2h_count_2h_tier"),
    )
    .agg(
        ((F.unix_timestamp(F.max("log_ts")) - F.unix_timestamp(F.min("log_ts"))) / 60).alias("span_minutes"),
        F.count("*").alias("num_rows"),
        F.min("log_ts").alias("started_at"),
        F.count("*").alias("event_count"),
    )
)

result = (
    summary
    .select(
        "hour_window",
        "latency_ms_sum_2h_count_2h_tier",
        "num_rows",
        "started_at",
        "event_count",
    )
    .orderBy(F.col("hour_window").desc(), "num_rows")
)
