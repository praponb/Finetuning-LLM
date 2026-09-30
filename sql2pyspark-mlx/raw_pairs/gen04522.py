from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("session_id").orderBy(F.col("duration_sec_rounded").desc())
w2 = Window.partitionBy("session_id", "event_type").orderBy(F.col("duration_sec_rounded").asc_nulls_first())
w3 = Window.partitionBy("event_type", "session_id")
w4 = Window.partitionBy("session_id").orderBy(F.col("duration_sec_rounded_rank").desc_nulls_first(), "id")

filtered = (
    df_web_events
    .filter(F.col("page").isNotNull() & F.col("event_type").isin(["page_view", "click"]))
    .select(
        "id",
        "session_id",
        "event_type",
        "page",
        F.date_format("event_time", "yyyy-MM-dd").alias("event_time_label"),
        F.round("duration_sec", 0).alias("duration_sec_rounded"),
    )
)

with_windows = (
    filtered
    .filter(F.col("page") == "/product")
    .select(
        "id",
        "session_id",
        F.dense_rank().over(w).alias("duration_sec_rounded_dense_rank"),
        F.rank().over(w2).alias("duration_sec_rounded_rank"),
        F.count("duration_sec_rounded").over(w3).alias("event_type_count_duration_sec_rounded"),
    )
)

ranked = (
    with_windows
    .withColumn("row_num", F.row_number().over(w4))
    .filter(F.col("row_num") == 1)
    .drop("row_num")
)

with_dims = (
    ranked
    .alias("a")
    .join(df_sessions.alias("d"), F.col("a.session_id") == F.col("d.id"), "inner")
    .select(
        "a.id",
        "a.session_id",
        "a.duration_sec_rounded_dense_rank",
        "a.event_type_count_duration_sec_rounded",
        F.col("d.device"),
        F.col("d.country"),
        F.col("d.browser"),
    )
)

matched_keys = (
    with_dims
    .select("session_id")
    .subtract(
        df_web_events
        .filter(F.col("duration_sec").between(229, 230))
        .select("session_id")
    )
)

kept_rows = (
    with_dims
    .alias("s")
    .join(matched_keys.alias("k"), (F.col("s.session_id") == F.col("k.session_id")), "inner")
    .select("s.*")
)

totals = (
    kept_rows
    .groupBy(
        F.when(F.col("duration_sec_rounded_dense_rank") >= 7, "PREMIUM").otherwise("STANDARD").alias("duration_sec_rounded_dense_rank_tier"),
    )
    .agg(
        F.avg(F.when(F.col("country") == "TH", F.col("event_type_count_duration_sec_rounded"))).alias("th_avg_event_type_count_duration_sec_rounded"),
        F.round(F.avg("duration_sec_rounded_dense_rank"), 2).alias("avg_duration_sec_rounded_dense_rank"),
        F.sum(F.when(F.col("device") == "mobile", F.col("event_type_count_duration_sec_rounded"))).alias("mobile_total_event_type_count_duration_sec_rounded"),
        F.percentile_approx("duration_sec_rounded_dense_rank", [0.25, 0.5, 0.75]).alias("duration_sec_rounded_dense_rank_quartiles"),
    )
    .filter(F.col("avg_duration_sec_rounded_dense_rank") >= 0)
)

result = (
    totals
    .select(
        "duration_sec_rounded_dense_rank_tier",
        "th_avg_event_type_count_duration_sec_rounded",
        "mobile_total_event_type_count_duration_sec_rounded",
        "duration_sec_rounded_dense_rank_quartiles",
    )
)
