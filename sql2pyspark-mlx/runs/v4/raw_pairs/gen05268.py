from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.orderBy(F.col("log_ts").cast("bigint")).rangeBetween(-3600, Window.currentRow)
w2 = Window.partitionBy("key")
w3 = Window.partitionBy("key").orderBy(F.col("is_flagged").asc_nulls_last())

lines = (
    df_app_logs
    .select(
        "id",
        "log_ts",
        "latency_ms",
        "status_code",
        F.explode_outer("context").alias("key", "attr_value"),
    )
)

parsed = (
    lines
    .select(
        "log_ts",
        "status_code",
        "key",
        "attr_value",
        F.shiftright("status_code", 2).alias("status_code_high"),
        (F.col("status_code").bitwiseAND(1) != 0).cast("int").alias("is_flagged"),
    )
)

enriched = (
    parsed
    .select(
        "*",
        F.count("status_code_high").over(w).alias("status_code_high_count_1h"),
        F.round((F.col("status_code") / F.sum("status_code").over(w2)), 4).alias("status_code_share"),
    )
)

enriched_app_logs = (
    enriched
    .select(
        "*",
        F.sum("status_code_share").over(w2).alias("key_sum_status_code_share"),
        F.count("is_flagged").over(w2).alias("key_count_is_flagged"),
    )
)

with_windows = (
    enriched_app_logs
    .select(
        "*",
        F.cume_dist().over(w3).alias("is_flagged_cume_dist"),
        F.count("key_count_is_flagged").over(w2).alias("key_count_key_count_is_flagged"),
    )
)

result = (
    with_windows
    .groupBy(
        F.date_trunc("DAY", "log_ts").alias("activity_day"),
        F.when(F.col("status_code_high") >= 2, "LARGE").otherwise("SMALL").alias("status_code_high_tier"),
        "key",
    )
    .agg(
        F.countDistinct("key").alias("distinct_keys"),
        F.max("is_flagged_cume_dist").alias("max_is_flagged_cume_dist"),
        F.sort_array(F.collect_set("key")).alias("key_list"),
        ((F.unix_timestamp(F.max("log_ts")) - F.unix_timestamp(F.min("log_ts"))) / 60).alias("duration_min"),
        F.count("*").alias("cnt"),
        F.count(F.when(F.col("key") == "trace", 1)).alias("trace_count"),
    )
    .orderBy(F.col("distinct_keys").desc())
)
