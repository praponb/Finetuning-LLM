from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_part = Window.partitionBy("host", "attr_key").orderBy("latency_ms", "id", "attr_key").rowsBetween(-2, Window.currentRow)
w2 = Window.partitionBy("attr_key")
w3 = Window.partitionBy("level", "attr_key").orderBy(F.col("last3_count_status_code").desc())
w4 = Window.partitionBy("attr_key").orderBy("rank_in_group", "id", "attr_key")
w5 = Window.partitionBy("host")
w6 = Window.partitionBy("level").orderBy("last3_count_status_code", "id", "attr_key").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w7 = Window.partitionBy("host", "attr_key").orderBy("corr_status_code_latency_ms", "id", "attr_key").rowsBetween(-3, -1)
w8 = Window.partitionBy("attr_key").orderBy("last3_sum_status_code", "id", "attr_key")
w9 = Window.orderBy(F.col("corr_status_code_latency_ms").desc(), "id", "attr_key")
w10 = Window.partitionBy("host", "attr_key")

exploded = (
    df_app_logs
    .select(
        "id",
        "host",
        "level",
        "latency_ms",
        "status_code",
        F.explode("context").alias("attr_key", "value"),
    )
)

enriched = (
    exploded
    .filter(F.col("latency_ms").isNotNull())
    .withColumn("last3_max_status_code", F.max("status_code").over(w_part))
    .withColumn("last3_sum_status_code", F.sum("status_code").over(w_part))
    .withColumn("last3_min_status_code", F.min("status_code").over(w_part))
    .withColumn("last3_count_status_code", F.count("status_code").over(w_part))
    .withColumn("corr_status_code_latency_ms", F.corr("status_code", "latency_ms").over(w2))
)

latest = (
    enriched
    .withColumn("rank_in_group", F.rank().over(w3))
    .filter(F.col("rank_in_group") == 1)
)

with_windows = (
    latest
    .select(
        "*",
        F.row_number().over(w4).alias("row_seq"),
        F.round((F.col("status_code") / F.sum("status_code").over(w5)), 4).alias("status_code_share"),
        F.last("attr_key", ignorenulls=True).over(w6).alias("last_known_attr_key"),
        F.avg("latency_ms").over(w7).alias("latency_ms_avg_3_prior"),
    )
)

enriched_app_logs = (
    with_windows
    .select(
        "*",
        F.count("status_code_share").over(w5).alias("host_count_status_code_share"),
        F.lag("corr_status_code_latency_ms", 2).over(w8).alias("prev_corr_status_code_latency_ms"),
    )
)

result = (
    enriched_app_logs
    .select(
        "id",
        "status_code",
        "attr_key",
        "last3_max_status_code",
        "last3_min_status_code",
        "last3_count_status_code",
        "corr_status_code_latency_ms",
        "last_known_attr_key",
        "latency_ms_avg_3_prior",
        "host_count_status_code_share",
        "prev_corr_status_code_latency_ms",
        F.row_number().over(w9).alias("row_seq_2"),
        F.count("host_count_status_code_share").over(w10).alias("host_count_host_count_status_code_share"),
        F.round((F.col("last3_sum_status_code") / F.sum("last3_sum_status_code").over(w5)), 4).alias("last3_sum_status_code_share"),
    )
)
