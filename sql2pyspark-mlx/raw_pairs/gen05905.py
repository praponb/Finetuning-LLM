from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("k").orderBy("log_ts", "id", "k").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w2 = Window.partitionBy("k").orderBy("status_code", "id", "k")
w3 = Window.partitionBy("host").orderBy("row_seq", "id", "k").rowsBetween(-3, Window.currentRow)
w4 = Window.partitionBy("host").orderBy(F.col("first_known_latency_ms").desc(), "id", "k")
w5 = Window.partitionBy("k").orderBy("row_seq", "id", "k").rowsBetween(-6, Window.currentRow)

flattened = (
    df_app_logs
    .select(
        "id",
        "host",
        "log_ts",
        "level",
        "latency_ms",
        "status_code",
        F.explode("context").alias("k", "v"),
    )
)

with_windows = (
    flattened
    .select(
        "id",
        "host",
        "status_code",
        "k",
        F.first("latency_ms", ignorenulls=True).over(w).alias("first_known_latency_ms"),
        F.row_number().over(w2).alias("row_seq"),
    )
)

with_windows_app_logs = (
    with_windows
    .select(
        "id",
        "status_code",
        "k",
        "first_known_latency_ms",
        "row_seq",
        F.min("row_seq").over(w3).alias("last4_min_row_seq"),
        F.count("row_seq").over(w3).alias("last4_count_row_seq"),
        F.sum("row_seq").over(w3).alias("last4_sum_row_seq"),
        F.row_number().over(w4).alias("row_seq_2"),
    )
)

enriched = (
    with_windows_app_logs
    .select(
        "id",
        "status_code",
        "k",
        "first_known_latency_ms",
        "row_seq",
        "last4_min_row_seq",
        "last4_count_row_seq",
        "last4_sum_row_seq",
        "row_seq_2",
        F.sum("last4_min_row_seq").over(w5).alias("last7_sum_last4_min_row_seq"),
        F.max("last4_min_row_seq").over(w5).alias("last7_max_last4_min_row_seq"),
        F.min("last4_min_row_seq").over(w5).alias("last7_min_last4_min_row_seq"),
    )
)

result = (
    enriched
    .groupBy("k")
    .agg(
        F.sort_array(F.collect_set("k")).alias("k_list"),
        F.round(F.sum("last4_sum_row_seq") / F.nullif(F.count(F.when(F.col("k") == "version", 1)), F.lit(0)), 2).alias("last4_sum_row_seq_per_version"),
        F.bit_and("status_code").alias("status_code_bit_and"),
        F.avg(F.when(F.col("k") == "region", F.col("last4_count_row_seq"))).alias("region_avg_last4_count_row_seq"),
    )
    .filter(F.col("status_code_bit_and") >= 1)
    .orderBy(F.col("region_avg_last4_count_row_seq").desc())
)
