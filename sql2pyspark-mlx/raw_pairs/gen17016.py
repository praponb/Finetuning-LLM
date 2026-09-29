from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.orderBy("duration_sec", "id")
w2 = Window.partitionBy("session_id").orderBy(F.col("duration_sec").asc_nulls_first())
w3 = Window.partitionBy("session_id").orderBy("duration_sec", "id").rowsBetween(-2, -1)
w4 = Window.partitionBy("event_type").orderBy(F.col("event_time").desc(), "id")
w_part = Window.partitionBy("event_type").orderBy("event_time", "id")
w6 = Window.partitionBy("page").orderBy("duration_sec", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w7 = Window.partitionBy("event_type").orderBy(F.col("seq_no").desc_nulls_last(), "id")
w8 = Window.partitionBy("page")
w9 = Window.partitionBy("page", "event_type").orderBy("running_min_prev_duration_sec", "id").rowsBetween(-2, -1)

base = (
    df_web_events
    .filter(F.col("page").isin(["/search", "/home", "/product"]))
    .select(
        "*",
        F.concat_ws("-", "event_type", "page").alias("event_type_page_key"),
        F.coalesce("page", F.lit("OTHER")).alias("page_clean"),
    )
)

with_windows = (
    base
    .select(
        "*",
        F.ntile(3).over(w).alias("duration_sec_bucket"),
        F.cume_dist().over(w2).alias("duration_sec_cume_dist"),
        F.avg("duration_sec").over(w3).alias("duration_sec_avg_2_prior"),
        F.row_number().over(w4).alias("seq_no"),
        F.lag("duration_sec", 2).over(w_part).alias("prev_duration_sec"),
    )
)

metrics = (
    with_windows
    .select(
        "*",
        F.max("prev_duration_sec").over(w6).alias("running_max_prev_duration_sec"),
        F.sum("prev_duration_sec").over(w6).alias("running_sum_prev_duration_sec"),
        F.count("prev_duration_sec").over(w6).alias("running_count_prev_duration_sec"),
        F.min("prev_duration_sec").over(w6).alias("running_min_prev_duration_sec"),
    )
)

deduped = (
    metrics
    .withColumn("dedup_rank", F.row_number().over(w7))
    .filter(F.col("dedup_rank") == 1)
    .select(
        "id",
        "event_type",
        "page",
        "seq_no",
        "running_max_prev_duration_sec",
        "running_sum_prev_duration_sec",
        "running_min_prev_duration_sec",
    )
)

scored = (
    deduped
    .select(
        "id",
        "event_type",
        "seq_no",
        "running_sum_prev_duration_sec",
        "running_min_prev_duration_sec",
        F.max("running_min_prev_duration_sec").over(w8).alias("page_max_running_min_prev_duration_sec"),
        F.avg("running_min_prev_duration_sec").over(w9).alias("running_min_prev_duration_sec_avg_2_prior"),
    )
)

result = (
    scored
    .select(
        "id",
        "event_type",
        F.col("running_min_prev_duration_sec").alias("RUNNING_MIN_PREV_DURATION_SEC"),
        F.col("running_sum_prev_duration_sec").alias("RUNNING_SUM_PREV_DURATION_SEC_VAL"),
    )
    .unpivot(
        ids=["id", "event_type"],
        values=["RUNNING_MIN_PREV_DURATION_SEC", "RUNNING_SUM_PREV_DURATION_SEC_VAL"],
        variableColumnName="metric_name",
        valueColumnName="val",
    )
    .filter(F.col("val").isNotNull())
)
