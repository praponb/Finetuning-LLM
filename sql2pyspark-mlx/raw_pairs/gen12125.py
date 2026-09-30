from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("key").orderBy("status_code_high", "id", "key")
w2 = Window.partitionBy("key").orderBy(F.col("has_fault").desc(), "id", "key")
w3 = Window.partitionBy("key").orderBy("event_seq_2", "id", "key").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w4 = Window.partitionBy("key").orderBy("event_seq", "id", "key")
w5 = Window.partitionBy("key").orderBy("next_status_code", "id", "key")
w6 = Window.partitionBy("key").orderBy("running_count_next_latency_ms", "id", "key").rowsBetween(Window.unboundedPreceding, Window.currentRow)

flattened = (
    df_app_logs
    .select(
        "id",
        "level",
        "latency_ms",
        "status_code",
        F.explode("context").alias("key", "v"),
    )
)

parsed = (
    flattened
    .select(
        "id",
        "latency_ms",
        "status_code",
        "key",
        "v",
        F.shiftright("status_code", 2).alias("status_code_high"),
        (F.col("status_code").bitwiseAND(8) != 0).cast("int").alias("has_fault"),
    )
)

enriched = (
    parsed
    .filter(F.col("has_fault") > 1)
    .select(
        "id",
        "latency_ms",
        "status_code",
        "key",
        "v",
        F.lag("status_code").over(w).alias("prev_status_code"),
        F.lead("status_code").over(w).alias("next_status_code"),
        F.lead("latency_ms").over(w).alias("next_latency_ms"),
        F.lag("latency_ms").over(w).alias("prev_latency_ms"),
        F.row_number().over(w).alias("event_seq"),
        F.row_number().over(w2).alias("event_seq_2"),
    )
)

enriched_app_logs = (
    enriched
    .select(
        "id",
        "status_code",
        "key",
        "v",
        "prev_status_code",
        "next_status_code",
        "prev_latency_ms",
        "event_seq",
        "event_seq_2",
        F.sum("next_latency_ms").over(w3).alias("running_sum_next_latency_ms"),
        F.min("next_latency_ms").over(w3).alias("running_min_next_latency_ms"),
        F.count("next_latency_ms").over(w3).alias("running_count_next_latency_ms"),
        F.row_number().over(w4).alias("row_seq"),
    )
)

enriched_app_logs_2 = (
    enriched_app_logs
    .select(
        "id",
        "key",
        "v",
        "prev_status_code",
        "event_seq",
        "running_min_next_latency_ms",
        "row_seq",
        F.lag("running_sum_next_latency_ms").over(w5).alias("prev_running_sum_next_latency_ms"),
        F.lead("running_sum_next_latency_ms").over(w5).alias("next_running_sum_next_latency_ms"),
        F.lead("event_seq").over(w5).alias("next_event_seq"),
        F.lag("event_seq").over(w5).alias("prev_event_seq"),
        F.row_number().over(w5).alias("event_seq_3"),
        F.last("running_count_next_latency_ms", ignorenulls=True).over(w6).alias("last_known_running_count_next_latency_ms"),
    )
)

result = (
    enriched_app_logs_2
    .withColumn("prev_status_code_tier", F.when(F.col("prev_status_code") >= 418, "HIGH").otherwise("LOW"))
    .groupingSets(
        [["prev_status_code_tier", "key"], ["prev_status_code_tier"]],
        "prev_status_code_tier",
        "key",
    )
    .agg(
        F.round(F.avg("row_seq"), 2).alias("avg_row_seq"),
        F.count("*").alias("num_rows"),
        F.grouping_id().alias("gid"),
    )
    .filter(F.col("avg_row_seq") < 1000000)
)
