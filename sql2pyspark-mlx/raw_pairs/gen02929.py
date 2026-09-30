from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("host")
w2 = Window.orderBy(F.col("log_ts").desc(), "id", "key")
w3 = Window.orderBy(F.col("seq_no").desc())
w4 = Window.partitionBy("key").orderBy("latency_ms_share", "id", "key").rowsBetween(-5, -1)
w5 = Window.partitionBy("service").orderBy("latency_ms_share", "id", "key").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w6 = Window.partitionBy("key")
w7 = Window.partitionBy("service").orderBy(F.col("latency_ms_share").desc(), "id", "key")
w8 = Window.partitionBy("key").orderBy("seq_no_share", "id", "key")
w9 = Window.partitionBy("key").orderBy("seq_no_avg_5_prior", "id", "key").rowsBetween(Window.unboundedPreceding, Window.currentRow)

exploded = (
    df_app_logs
    .select(
        "id",
        "service",
        "host",
        "log_ts",
        "latency_ms",
        "status_code",
        F.explode("context").alias("key", "v"),
    )
)

metrics = (
    exploded
    .select(
        "id",
        "service",
        "key",
        "v",
        F.round((F.col("latency_ms") / F.sum("latency_ms").over(w)), 4).alias("latency_ms_share"),
        F.row_number().over(w2).alias("seq_no"),
    )
)

scored = (
    metrics
    .filter(F.col("service") == "billing")
    .select(
        "id",
        "key",
        "v",
        "latency_ms_share",
        F.rank().over(w3).alias("seq_no_rank"),
        F.avg("seq_no").over(w4).alias("seq_no_avg_5_prior"),
        F.first("seq_no", ignorenulls=True).over(w5).alias("first_known_seq_no"),
        F.round((F.col("seq_no") / F.sum("seq_no").over(w6)), 4).alias("seq_no_share"),
        F.ntile(4).over(w7).alias("latency_ms_share_bucket"),
    )
)

with_windows = (
    scored
    .withColumn("seq_no_share_bucket", F.ntile(3).over(w8))
    .withColumn("cumulative_seq_no_rank", F.sum("seq_no_rank").over(w9))
    .withColumn("running_seq_no_share", F.sum("seq_no_share").over(w9))
)

result = (
    with_windows
    .cube("key")
    .agg(
        F.round(F.avg("running_seq_no_share"), 2).alias("avg_running_seq_no_share"),
        F.size(F.collect_set("key")).alias("num_keys"),
        F.percentile_approx("latency_ms_share_bucket", [0.25, 0.5, 0.75]).alias("latency_ms_share_bucket_quartiles"),
        F.sort_array(F.collect_set("key")).alias("key_list"),
        F.avg("running_seq_no_share").alias("avg_running_seq_no_share_2"),
    )
    .orderBy(F.col("key").asc_nulls_last())
)
