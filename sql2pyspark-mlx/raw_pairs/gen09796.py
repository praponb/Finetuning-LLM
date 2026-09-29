from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("attr_key")
w2 = Window.partitionBy("attr_key").orderBy(F.col("status_code").desc(), "id", "attr_key")
w3 = Window.partitionBy("attr_key", "level").orderBy(F.col("status_code_share").desc_nulls_last(), "id", "attr_key")
w4 = Window.partitionBy("level", "attr_key")
w5 = Window.partitionBy("attr_key").orderBy("latency_ms", "id", "attr_key").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w6 = Window.orderBy("status_code", "id", "attr_key").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w7 = Window.orderBy(F.col("status_code_share").desc_nulls_first())
w_grp = Window.orderBy("status_code_share", "id", "attr_key").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w9 = Window.partitionBy("attr_key").orderBy("last_known_status_code_share", "id", "attr_key")
w10 = Window.orderBy(F.col("prev_latency_ms").desc(), "id", "attr_key")
w11 = Window.partitionBy("attr_key").orderBy("status_code_share_cume_dist", "id", "attr_key").rowsBetween(-6, Window.currentRow)
w12 = Window.partitionBy("attr_key").orderBy(F.col("status_code_bucket_share").desc_nulls_first())

lines = (
    df_app_logs
    .select(
        "id",
        "level",
        "latency_ms",
        "status_code",
        F.explode("context").alias("attr_key", "v"),
    )
)

enriched = (
    lines
    .filter(F.col("level") == "DEBUG")
    .select(
        "*",
        F.round((F.col("status_code") / F.sum("status_code").over(w)), 4).alias("status_code_share"),
        F.corr("latency_ms", "status_code").over(w).alias("corr_latency_ms_status_code"),
        F.ntile(10).over(w2).alias("status_code_bucket"),
    )
)

latest = (
    enriched
    .withColumn("rn", F.row_number().over(w3))
    .filter(F.col("rn") == 1)
)

metrics = (
    latest
    .select(
        "id",
        "latency_ms",
        "status_code",
        "attr_key",
        "v",
        "corr_latency_ms_status_code",
        F.round((F.col("status_code_bucket") / F.sum("status_code_bucket").over(w4)), 4).alias("status_code_bucket_share"),
        F.first("corr_latency_ms_status_code", ignorenulls=True).over(w5).alias("first_known_corr_latency_ms_status_code"),
        F.sum("status_code").over(w6).alias("cumulative_status_code"),
        F.cume_dist().over(w7).alias("status_code_share_cume_dist"),
        F.last("status_code_share", ignorenulls=True).over(w_grp).alias("last_known_status_code_share"),
    )
)

metrics_app_logs = (
    metrics
    .withColumn("prev_latency_ms", F.lag("latency_ms").over(w9))
    .withColumn("prev_status_code_bucket_share", F.lag("status_code_bucket_share").over(w9))
    .withColumn("next_status_code_bucket_share", F.lead("status_code_bucket_share").over(w9))
    .withColumn("seq_no", F.row_number().over(w9))
)

scored = (
    metrics_app_logs
    .select(
        "id",
        "status_code",
        "attr_key",
        "corr_latency_ms_status_code",
        "status_code_bucket_share",
        "cumulative_status_code",
        "status_code_share_cume_dist",
        "last_known_status_code_share",
        "prev_status_code_bucket_share",
        F.ntile(4).over(w10).alias("prev_latency_ms_bucket"),
        F.avg("status_code_bucket_share").over(w11).alias("status_code_bucket_share_avg_6"),
        F.percent_rank().over(w12).alias("status_code_bucket_share_percent_rank"),
    )
)

base = (
    scored
    .filter(F.col("status_code_bucket_share") <= 0.29)
    .withColumn("cumulative_status_code_minus_last_known_status_code_share", (F.col("cumulative_status_code") - F.col("last_known_status_code_share")))
    .withColumn("last_known_status_code_share_minus_status_code_bucket_share_percent_rank", (F.col("last_known_status_code_share") - F.col("status_code_bucket_share_percent_rank")))
)

totals = (
    base
    .groupBy("attr_key")
    .agg(
        F.round(F.sum("prev_latency_ms_bucket") / F.nullif(F.count(F.when(F.col("attr_key") == "region", 1)), F.lit(0)), 2).alias("prev_latency_ms_bucket_per_region"),
        F.round(F.avg("prev_status_code_bucket_share"), 2).alias("avg_prev_status_code_bucket_share"),
    )
)

result = (
    totals
    .select(
        "attr_key",
        "prev_latency_ms_bucket_per_region",
        "avg_prev_status_code_bucket_share",
        F.when(F.col("attr_key").rlike("^(region|trace)$"), "OTHER_ZONE").otherwise("GROUP_B").alias("attr_key_zone"),
    )
    .orderBy(F.col("avg_prev_status_code_bucket_share").asc_nulls_last())
)
