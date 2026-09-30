from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("key").orderBy("status_code", "id", "key")
w2 = Window.partitionBy("service")
w3 = Window.partitionBy("service", "key")
w4 = Window.partitionBy("service", "key").orderBy("next_latency_ms", "id", "key")
w5 = Window.partitionBy("service").orderBy(F.col("corr_status_code_latency_ms").desc(), "id", "key")
w6 = Window.partitionBy("key")
w7 = Window.partitionBy("key", "service").orderBy("latency_ms", "id", "key").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w8 = Window.partitionBy("key").orderBy("key_avg_latency_ms_share", "id", "key").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w9 = Window.partitionBy("key").orderBy("corr_status_code_latency_ms", "id", "key").rowsBetween(-5, Window.currentRow)
w10 = Window.orderBy(F.col("next_latency_ms").desc(), "id", "key")
w11 = Window.partitionBy("service").orderBy("latency_ms", "id", "key")
w_grp = Window.orderBy(F.col("key_avg_latency_ms_share").desc(), "id", "key")
w13 = Window.partitionBy("key").orderBy("corr_status_code_latency_ms", "id", "key")
w14 = Window.partitionBy("key").orderBy(F.col("next_next_latency_ms_bucket").desc_nulls_last(), "id", "key")
w15 = Window.orderBy(F.col("service_max_latency_ms").desc(), "id", "key")

exploded = (
    df_app_logs
    .select(
        "id",
        "service",
        "host",
        "latency_ms",
        "status_code",
        F.explode("context").alias("key", "value"),
    )
)

enriched = (
    exploded
    .filter(F.col("service") == "search")
    .select(
        "id",
        "service",
        "latency_ms",
        "key",
        "value",
        F.lead("latency_ms", 1, 0).over(w).alias("next_latency_ms"),
        F.corr("status_code", "latency_ms").over(w2).alias("corr_status_code_latency_ms"),
        F.round((F.col("latency_ms") / F.sum("latency_ms").over(w3)), 4).alias("latency_ms_share"),
    )
)

scored = (
    enriched
    .select(
        "*",
        F.lag("latency_ms_share", 1).over(w4).alias("prev_latency_ms_share"),
        F.max("latency_ms").over(w2).alias("service_max_latency_ms"),
        F.ntile(10).over(w5).alias("corr_status_code_latency_ms_bucket"),
        F.avg("latency_ms_share").over(w6).alias("key_avg_latency_ms_share"),
        F.last("latency_ms_share", ignorenulls=True).over(w7).alias("last_known_latency_ms_share"),
    )
)

scored_app_logs = (
    scored
    .select(
        "id",
        "key",
        "next_latency_ms",
        "corr_status_code_latency_ms",
        "latency_ms_share",
        "service_max_latency_ms",
        "corr_status_code_latency_ms_bucket",
        F.last("prev_latency_ms_share", ignorenulls=True).over(w8).alias("last_known_prev_latency_ms_share"),
        F.avg("key_avg_latency_ms_share").over(w9).alias("key_avg_latency_ms_share_avg_5"),
        F.ntile(4).over(w10).alias("next_latency_ms_bucket"),
        F.row_number().over(w11).alias("event_seq"),
        F.ntile(10).over(w_grp).alias("key_avg_latency_ms_share_bucket"),
    )
)

scored_app_logs_2 = (
    scored_app_logs
    .select(
        "id",
        "key",
        "next_latency_ms",
        "service_max_latency_ms",
        "last_known_prev_latency_ms_share",
        "key_avg_latency_ms_share_avg_5",
        "next_latency_ms_bucket",
        "key_avg_latency_ms_share_bucket",
        F.lead("next_latency_ms_bucket").over(w13).alias("next_next_latency_ms_bucket"),
        F.lead("key_avg_latency_ms_share_bucket").over(w13).alias("next_key_avg_latency_ms_share_bucket"),
        F.row_number().over(w13).alias("seq_no"),
    )
)

ranked = (
    scored_app_logs_2
    .withColumn("latest_rank", F.row_number().over(w14))
    .filter(F.col("latest_rank") == 1)
    .select(
        "id",
        "key",
        "next_latency_ms",
        "service_max_latency_ms",
        "last_known_prev_latency_ms_share",
        "key_avg_latency_ms_share_avg_5",
        "key_avg_latency_ms_share_bucket",
        "next_next_latency_ms_bucket",
        "seq_no",
    )
)

scored_app_logs_3 = (
    ranked
    .withColumn("row_seq", F.row_number().over(w15))
    .withColumn("key_avg_seq_no", F.avg("seq_no").over(w6))
)

result = (
    scored_app_logs_3
    .groupingSets([["key", "key_avg_seq_no"], ["key"]], "key", "key_avg_seq_no")
    .agg(
        F.size(F.collect_set("key")).alias("num_keys"),
        F.round(F.sum("next_next_latency_ms_bucket") / F.nullif(F.count(F.when(F.col("key") == "trace", 1)), F.lit(0)), 2).alias("next_next_latency_ms_bucket_per_trace"),
        F.percentile_approx("next_latency_ms", 0.5).alias("next_latency_ms_p50"),
    )
    .orderBy(F.col("key").desc())
)
