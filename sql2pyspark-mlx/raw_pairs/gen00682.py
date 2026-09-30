from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("host").orderBy("log_ts", "id", "key")
w2 = Window.partitionBy("host").orderBy("log_ts", "id", "key").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w3 = Window.partitionBy("key").orderBy(F.col("latency_ms").desc_nulls_last(), "id", "key")
w4 = Window.partitionBy("service", "host").orderBy("latency_ms", "id", "key")
w_part = Window.partitionBy("key", "host").orderBy("session_no", "id", "key")
w6 = Window.partitionBy("service").orderBy(F.col("min_key_length").desc())

exploded = (
    df_app_logs
    .select(
        "id",
        "service",
        "host",
        "log_ts",
        "level",
        "latency_ms",
        F.explode("context").alias("key", "v"),
    )
)

gaps = (
    exploded
    .withColumn("new_group_flag", F.when(F.lag("log_ts").over(w).isNull() | (F.col("log_ts") > F.lag("log_ts").over(w) + F.expr("INTERVAL 20 MINUTES")), 1).otherwise(0))
)

sessions_app_logs = gaps.withColumn("session_no", F.sum("new_group_flag").over(w2))

ranked = (
    sessions_app_logs
    .withColumn("dedup_rank", F.row_number().over(w3))
    .filter(F.col("dedup_rank") == 1)
    .drop("dedup_rank")
)

with_windows = (
    ranked
    .select(
        "id",
        "service",
        "host",
        "level",
        "latency_ms",
        "key",
        "new_group_flag",
        "session_no",
        F.ntile(5).over(w4).alias("latency_ms_bucket"),
        F.ntile(5).over(w_part).alias("session_no_bucket"),
    )
)

kept_keys = (
    with_windows
    .select("service")
    .intersectAll(
        df_app_logs
        .filter(F.col("host") == "h2")
        .select("service")
    )
)

reconciled = (
    with_windows
    .alias("s")
    .join(kept_keys.alias("k"), (F.col("s.service") == F.col("k.service")), "inner")
    .select("s.*")
)

valid = (
    reconciled
    .filter(F.col("latency_ms_bucket") == 8)
    .select(
        "id",
        "service",
        "host",
        "level",
        "latency_ms",
        "key",
        "new_group_flag",
        "session_no",
        "latency_ms_bucket",
        "session_no_bucket",
        F.substring("level", 1, 2).alias("level_prefix"),
        F.length("key").alias("key_length"),
    )
)

base = (
    valid
    .filter(F.col("key").isNotNull())
    .withColumn("new_group_flag_plus_key_length", (F.col("new_group_flag") + F.col("key_length")))
    .withColumn("key_upper", F.upper("key"))
    .withColumn("resolved_key", F.coalesce("key", F.lit("NONE")))
)

result = (
    base
    .groupBy(
        "service",
        "level",
        F.when(F.col("new_group_flag_plus_key_length") >= 66, "PREMIUM").otherwise("STANDARD").alias("new_group_flag_plus_key_length_tier"),
    )
    .agg(
        F.count("*").alias("num_rows"),
        F.size(F.collect_set("level")).alias("num_levels"),
        F.min("key_length").alias("min_key_length"),
        F.count(F.when(F.col("key") == "trace", 1)).alias("trace_rows"),
        F.round(F.sum("new_group_flag_plus_key_length") / F.nullif(F.count(F.when(F.col("host") == "h4", 1)), F.lit(0)), 2).alias("new_group_flag_plus_key_length_per_h4"),
    )
    .filter(F.col("num_levels") > 1)
    .withColumn("min_key_length_dense_rank", F.dense_rank().over(w6))
    .orderBy("new_group_flag_plus_key_length_tier")
)
