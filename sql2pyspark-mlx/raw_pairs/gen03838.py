from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("key", "host").orderBy(F.col("latency_ms").asc_nulls_last())
w2 = Window.orderBy("latency_ms", "id", "key")
w3 = Window.partitionBy("key", "host").orderBy("latency_ms", "id", "key")
w4 = Window.partitionBy("host").orderBy(F.col("log_ts").cast("bigint")).rangeBetween(-3600, Window.currentRow)
w5 = Window.partitionBy("host").orderBy("log_ts", "id", "key").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_ordered = Window.partitionBy("host").orderBy("latency_ms", "id", "key")
w7 = Window.partitionBy("host").orderBy(F.col("latency_ms_dense_rank").desc(), "id", "key")
w8 = Window.partitionBy("host").orderBy("latency_ms", "id", "key").rowsBetween(-2, Window.currentRow)
w9 = Window.partitionBy("key").orderBy("latency_ms_avg_1h", "id", "key").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w10 = Window.partitionBy("key").orderBy(F.col("last3_avg_event_seq").desc_nulls_last(), "id", "key")

lines = (
    df_app_logs
    .select(
        "id",
        "host",
        "log_ts",
        "latency_ms",
        "status_code",
        F.explode("context").alias("key", "attr_value"),
    )
)

scored = (
    lines
    .select(
        "*",
        F.dense_rank().over(w).alias("latency_ms_dense_rank"),
        F.lead("latency_ms", 2).over(w2).alias("next_latency_ms"),
        F.lead("status_code", 2).over(w3).alias("next_status_code"),
        F.avg("latency_ms").over(w4).alias("latency_ms_avg_1h"),
        F.sum("latency_ms").over(w5).alias("cumulative_latency_ms"),
    )
)

base = (
    scored
    .filter(F.col("key").isNotNull())
    .select(
        "id",
        "host",
        "log_ts",
        "latency_ms",
        "status_code",
        "key",
        "latency_ms_dense_rank",
        "next_latency_ms",
        "latency_ms_avg_1h",
        F.upper("key").alias("key_upper"),
        F.coalesce("key", F.lit("NONE")).alias("key_clean"),
    )
)

enriched = (
    base
    .select(
        "id",
        "host",
        "latency_ms",
        "key",
        "next_latency_ms",
        "latency_ms_avg_1h",
        "key_clean",
        F.lead("latency_ms_avg_1h").over(w_ordered).alias("next_latency_ms_avg_1h"),
        F.lag("status_code").over(w_ordered).alias("prev_status_code"),
        F.row_number().over(w_ordered).alias("event_seq"),
        F.ntile(4).over(w7).alias("latency_ms_dense_rank_bucket"),
    )
)

metrics = (
    enriched
    .select(
        "*",
        F.sum("event_seq").over(w8).alias("last3_sum_event_seq"),
        F.max("event_seq").over(w8).alias("last3_max_event_seq"),
        F.avg("event_seq").over(w8).alias("last3_avg_event_seq"),
        F.last("next_latency_ms", ignorenulls=True).over(w9).alias("last_known_next_latency_ms"),
    )
)

latest = (
    metrics
    .withColumn("latest_rank", F.row_number().over(w10))
    .filter(F.col("latest_rank") == 1)
)

valid = (
    latest
    .filter(F.col("key").isNotNull())
    .select(
        "id",
        "host",
        "key",
        "key_clean",
        "next_latency_ms_avg_1h",
        "event_seq",
        "last3_sum_event_seq",
        "last3_max_event_seq",
        "last_known_next_latency_ms",
        F.concat("key", F.lit(", "), "host").alias("key_host"),
        F.coalesce("key", F.lit("NONE")).alias("key_clean_2"),
    )
)

result = (
    valid
    .select(
        "id",
        "host",
        "key",
        "next_latency_ms_avg_1h",
        "last3_max_event_seq",
        "key_host",
        "key_clean_2",
        F.when(F.col("key").rlike("^(region|trace)$"), "PRIORITY").when(F.col("key").rlike("^(version)$"), "GROUP_A").otherwise("CORE").alias("key_zone"),
    )
    .orderBy(F.col("next_latency_ms_avg_1h").asc_nulls_first())
)
