from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("service").orderBy(F.col("log_ts").cast("bigint")).rangeBetween(-900, Window.currentRow)
w2 = Window.partitionBy("service").orderBy(F.col("log_ts").cast("bigint")).rangeBetween(-7200, Window.currentRow)
w3 = Window.partitionBy("service").orderBy(F.col("log_ts_weekofyear").desc_nulls_first())

flattened = (
    df_app_logs
    .select(
        "id",
        "service",
        "log_ts",
        "latency_ms",
        "status_code",
        F.explode_outer("context").alias("attr_key", "value"),
    )
)

remaining_keys = (
    flattened
    .select("service", "id")
    .intersect(
        df_app_logs
        .filter(F.col("level") == "ERROR")
        .select("service", "id")
    )
)

kept_rows = (
    flattened
    .alias("s")
    .join(remaining_keys.alias("k"), (F.col("s.service") == F.col("k.service")) & (F.col("s.id") == F.col("k.id")), "inner")
    .select("s.*")
)

valid = (
    kept_rows
    .filter(F.col("status_code") >= 263)
    .select(
        "id",
        "service",
        "log_ts",
        "value",
        F.weekofyear("log_ts").alias("log_ts_weekofyear"),
        F.when(F.col("service") == "checkout", 1).otherwise(0).alias("is_checkout"),
    )
)

metrics = (
    valid
    .withColumn("log_ts_weekofyear_count_15m", F.count("log_ts_weekofyear").over(w))
    .withColumn("log_ts_weekofyear_avg_2h", F.avg("log_ts_weekofyear").over(w2))
    .withColumn("log_ts_weekofyear_percent_rank", F.percent_rank().over(w3))
)

result = (
    metrics
    .groupBy(
        F.when(F.col("log_ts_weekofyear") >= 62, "PREMIUM").otherwise("STANDARD").alias("log_ts_weekofyear_tier"),
    )
    .agg(
        F.max(F.when(F.col("service") == "auth", F.col("log_ts_weekofyear_avg_2h"))).alias("auth_max_log_ts_weekofyear_avg_2h"),
        F.count(F.when(F.col("service") == "search", 1)).alias("search_count"),
        F.count("*").alias("cnt"),
        F.percentile_approx("log_ts_weekofyear_avg_2h", 0.95).alias("log_ts_weekofyear_avg_2h_p95"),
        F.round(F.avg("log_ts_weekofyear_count_15m"), 2).alias("avg_log_ts_weekofyear_count_15m"),
    )
    .orderBy(F.col("auth_max_log_ts_weekofyear_avg_2h").desc())
)
