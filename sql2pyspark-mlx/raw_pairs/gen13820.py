from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("key").orderBy("is_region", "id", "key")
w2 = Window.partitionBy(F.year("log_ts"), "service").orderBy("latency_ms", "id", "key")
w3 = Window.partitionBy("service").orderBy("latency_ms", "id", "key").rowsBetween(-5, -1)
w4 = Window.partitionBy("key").orderBy(F.col("is_debug").asc_nulls_last())
w5 = Window.partitionBy(F.to_date("log_ts"), "service").orderBy("is_region", "id", "key")
w6 = Window.partitionBy("service").orderBy("log_ts", "id", "key")
w7 = Window.partitionBy("service").orderBy("log_ts", "id", "key").rowsBetween(Window.unboundedPreceding, Window.currentRow)

flattened = (
    df_app_logs
    .select(
        "id",
        "service",
        "log_ts",
        "level",
        "latency_ms",
        F.explode("context").alias("key", "value"),
    )
)

filtered = (
    flattened
    .filter(F.col("level") == "WARN")
    .withColumn("is_region", F.when(F.col("key") == "region", 1).otherwise(0))
    .withColumn("is_debug", F.when(F.col("level") == "DEBUG", 1).otherwise(0))
)

with_windows = (
    filtered
    .select(
        "*",
        F.row_number().over(w).alias("row_seq"),
        F.lag("is_debug", 2, 0).over(w2).alias("prev_is_debug"),
        F.avg("latency_ms").over(w3).alias("latency_ms_avg_5_prior"),
        F.dense_rank().over(w4).alias("is_debug_dense_rank"),
        F.row_number().over(w5).alias("seq_no"),
    )
)

cleaned = (
    with_windows
    .filter((F.col("log_ts") > F.lit("2025-06-01").cast("date")) & (F.col("row_seq") > 7))
    .select(
        "*",
        F.coalesce("is_region", F.lit(0)).alias("is_region_filled"),
        F.coalesce("key", F.lit("N/A")).alias("key_filled"),
    )
)

with_gaps = (
    cleaned
    .withColumn("is_new_burst", F.when(F.lag("log_ts").over(w6).isNull() | (F.col("log_ts") > F.lag("log_ts").over(w6) + F.expr("INTERVAL 15 MINUTES")), 1).otherwise(0))
)

sessionized = with_gaps.withColumn("burst_id", F.sum("is_new_burst").over(w7))

filtered_app_logs = (
    sessionized
    .filter(F.col("log_ts") >= F.lit("2025-06-01").cast("date"))
    .select(
        "id",
        "service",
        "level",
        "latency_ms",
        "key",
        "value",
        "is_region",
        "is_debug",
        "row_seq",
        "prev_is_debug",
        "latency_ms_avg_5_prior",
        "seq_no",
        "is_region_filled",
        "is_new_burst",
        "burst_id",
        F.to_date("log_ts").alias("log_ts_date"),
        F.when(F.col("latency_ms") >= 2567, "High").when(F.col("latency_ms") >= 1165, "Medium").otherwise("Low").alias("latency_ms_band"),
        F.coalesce("key", F.lit("NONE")).alias("key_clean"),
    )
)

result = (
    filtered_app_logs
    .groupBy(
        F.when(F.col("latency_ms") >= 1855, "HIGH").otherwise("LOW").alias("latency_ms_tier"),
    )
    .agg(
        F.sort_array(F.collect_set("key")).alias("key_list"),
        F.sort_array(F.collect_set("level")).alias("level_list"),
        F.count_if(F.col("service") == "search").alias("search_events"),
    )
    .orderBy("latency_ms_tier")
)
