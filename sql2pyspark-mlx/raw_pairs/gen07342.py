from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("host").orderBy("latency_ms", "id", "key").rowsBetween(-3, Window.currentRow)
w2 = Window.partitionBy("host", "service").orderBy(F.col("latency_ms").desc(), "id", "key")
w3 = Window.orderBy(F.col("latency_ms").desc_nulls_last())
w4 = Window.partitionBy("service").orderBy(F.col("log_ts").desc(), "id", "key")
w5 = Window.partitionBy("key").orderBy(F.col("latency_ms_avg_3").asc_nulls_last(), "id", "key")
w6 = Window.orderBy("latency_ms_avg_3", "id", "key")
w7 = Window.partitionBy("key").orderBy("latency_ms", "id", "key")
w_grp = Window.partitionBy("key").orderBy("latency_ms_bucket_plus_latency_ms_dense_rank", "id", "key")

flattened = (
    df_app_logs
    .select(
        "id",
        "service",
        "host",
        "log_ts",
        "latency_ms",
        F.explode("context").alias("key", "attr_value"),
    )
)

enriched = (
    flattened
    .select(
        "id",
        "service",
        "latency_ms",
        "key",
        "attr_value",
        F.avg("latency_ms").over(w).alias("latency_ms_avg_3"),
        F.ntile(5).over(w2).alias("latency_ms_bucket"),
        F.dense_rank().over(w3).alias("latency_ms_dense_rank"),
        F.row_number().over(w4).alias("row_seq"),
    )
)

valid = (
    enriched
    .filter(F.col("key").isNotNull())
    .select(
        "id",
        "latency_ms",
        "key",
        "attr_value",
        "latency_ms_avg_3",
        "latency_ms_dense_rank",
        "row_seq",
        F.col("row_seq").cast("string").alias("row_seq_str"),
        (F.col("latency_ms_bucket") + F.col("latency_ms_dense_rank")).alias("latency_ms_bucket_plus_latency_ms_dense_rank"),
    )
)

latest = (
    valid
    .withColumn("dedup_rank", F.row_number().over(w5))
    .filter(F.col("dedup_rank") == 1)
)

metrics = (
    latest
    .withColumn("next_row_seq", F.lead("row_seq", 1, 0).over(w6))
    .withColumn("next_latency_ms", F.lead("latency_ms", 2).over(w7))
)

metrics_app_logs = (
    metrics
    .select(
        "id",
        "latency_ms",
        "key",
        "latency_ms_avg_3",
        "row_seq_str",
        "latency_ms_bucket_plus_latency_ms_dense_rank",
        "next_latency_ms",
        F.lag("row_seq").over(w_grp).alias("prev_row_seq"),
        F.lead("row_seq").over(w_grp).alias("next_row_seq_2"),
        F.lag("next_latency_ms").over(w_grp).alias("prev_next_latency_ms"),
        F.row_number().over(w_grp).alias("event_seq"),
    )
)

result = (
    metrics_app_logs
    .groupBy("key")
    .agg(
        F.count_if(F.col("key") == "version").alias("version_events"),
        F.sum(F.when(F.col("key") == "trace", F.col("prev_next_latency_ms"))).alias("trace_total_prev_next_latency_ms"),
        F.stddev_samp("latency_ms").alias("latency_ms_stddev"),
    )
    .filter(F.col("latency_ms_stddev") < 1000000)
    .orderBy(F.col("key").asc_nulls_first())
)
