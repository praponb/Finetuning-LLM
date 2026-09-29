from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("host").orderBy("status_code", "id", "k")
w2 = Window.partitionBy("k").orderBy("status_code", "id", "k").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w3 = Window.partitionBy("k").orderBy(F.col("prev_status_code").desc_nulls_first())
w4 = Window.partitionBy("host")
w5 = Window.partitionBy("host").orderBy("running_status_code", "id", "k").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_part = Window.partitionBy("k").orderBy("prev_status_code", "id", "k").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w7 = Window.partitionBy("service", "k")
w8 = Window.partitionBy("host", "service").orderBy("dedup_rank", "id", "k").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w9 = Window.partitionBy("host").orderBy("running_running_status_code", "id", "k").rowsBetween(-2, Window.currentRow)
w10 = Window.partitionBy("k").orderBy("running_running_status_code", "id", "k")
w11 = Window.partitionBy("status_code_share_tier").orderBy(F.col("num_hosts").desc())

flattened = (
    df_app_logs
    .select(
        "id",
        "service",
        "host",
        "status_code",
        F.explode("context").alias("k", "attr_value"),
    )
)

enriched = (
    flattened
    .filter(F.col("k") == "trace")
    .withColumn("prev_status_code", F.lag("status_code").over(w))
    .withColumn("event_seq", F.row_number().over(w))
    .withColumn("running_status_code", F.sum("status_code").over(w2))
)

deduped = (
    enriched
    .withColumn("dedup_rank", F.rank().over(w3))
    .filter(F.col("dedup_rank") == 1)
)

scored = (
    deduped
    .select(
        "id",
        "host",
        "k",
        "prev_status_code",
        "running_status_code",
        F.round((F.col("status_code") / F.sum("status_code").over(w4)), 4).alias("status_code_share"),
        F.first("k", ignorenulls=True).over(w5).alias("first_known_k"),
        F.last("prev_status_code", ignorenulls=True).over(w_part).alias("last_known_prev_status_code"),
        F.round((F.col("running_status_code") / F.sum("running_status_code").over(w7)), 4).alias("running_status_code_share"),
        F.sum("running_status_code").over(w8).alias("running_running_status_code"),
    )
)

cleaned = (
    scored
    .filter(F.col("k").isNotNull() & (F.col("running_status_code_share") >= 0.73))
    .withColumn("last_known_prev_status_code_adjusted", F.round((F.col("last_known_prev_status_code") * 0.9), 2))
)

enriched_app_logs = (
    cleaned
    .select(
        "id",
        "host",
        "k",
        "prev_status_code",
        "running_status_code",
        "status_code_share",
        "first_known_k",
        "running_status_code_share",
        "running_running_status_code",
        F.max("last_known_prev_status_code_adjusted").over(w9).alias("last3_max_last_known_prev_status_code_adjusted"),
        F.min("last_known_prev_status_code_adjusted").over(w9).alias("last3_min_last_known_prev_status_code_adjusted"),
        F.sum("last_known_prev_status_code_adjusted").over(w9).alias("last3_sum_last_known_prev_status_code_adjusted"),
        F.ntile(4).over(w10).alias("running_running_status_code_bucket"),
    )
)

filtered = (
    enriched_app_logs
    .filter(F.col("k").isNotNull() & F.col("last3_sum_last_known_prev_status_code_adjusted").between(170, 380))
    .select(
        "id",
        "host",
        "k",
        "prev_status_code",
        "running_status_code",
        "status_code_share",
        "first_known_k",
        "running_status_code_share",
        "running_running_status_code",
        "last3_max_last_known_prev_status_code_adjusted",
        "last3_min_last_known_prev_status_code_adjusted",
        "last3_sum_last_known_prev_status_code_adjusted",
        "running_running_status_code_bucket",
        F.round((F.col("running_status_code") * 0.9), 2).alias("running_status_code_adjusted"),
    )
)

result = (
    filtered
    .groupBy(
        F.when(F.col("status_code_share") >= 0.73, "PREMIUM").otherwise("STANDARD").alias("status_code_share_tier"),
        "host",
    )
    .agg(
        F.size(F.collect_set("host")).alias("num_hosts"),
        F.percentile_approx("running_status_code_share", 0.9).alias("running_status_code_share_p90"),
        F.count("*").alias("row_count"),
        F.percentile_approx("running_running_status_code_bucket", [0.25, 0.5, 0.75]).alias("running_running_status_code_bucket_quartiles"),
    )
    .withColumn("num_hosts_dense_rank", F.dense_rank().over(w11))
)
