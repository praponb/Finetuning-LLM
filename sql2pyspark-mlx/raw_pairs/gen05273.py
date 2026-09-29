from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("session_id").orderBy("duration_sec", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w2 = Window.partitionBy("session_id").orderBy("event_time", "id")
w3 = Window.partitionBy("session_id", "page").orderBy(F.col("duration_sec").desc_nulls_last())
w4 = Window.orderBy(F.col("duration_sec").asc_nulls_first())
w_hist = Window.partitionBy("page", "event_type").orderBy("running_max_duration_sec", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_part = Window.partitionBy("event_type")
w7 = Window.partitionBy("page").orderBy("event_time", "id")
w8 = Window.partitionBy("page").orderBy("event_time", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

cleaned = (
    df_web_events
    .filter(F.col("page").isNotNull())
    .select("*", F.coalesce("page", F.lit("Unknown")).alias("page_filled"))
)

scored = (
    cleaned
    .select(
        "*",
        F.sum("duration_sec").over(w).alias("running_sum_duration_sec"),
        F.max("duration_sec").over(w).alias("running_max_duration_sec"),
        F.count("duration_sec").over(w).alias("running_count_duration_sec"),
        F.row_number().over(w2).alias("row_seq"),
    )
)

scored_web_events = (
    scored
    .filter(F.col("event_type").isin(["purchase", "page_view"]))
    .select(
        "*",
        F.dense_rank().over(w3).alias("duration_sec_dense_rank"),
        F.cume_dist().over(w4).alias("duration_sec_cume_dist"),
        F.sum("running_count_duration_sec").over(w_hist).alias("cumulative_running_count_duration_sec"),
        F.count("duration_sec").over(w_part).alias("event_type_count_duration_sec"),
    )
)

with_dims = (
    scored_web_events
    .alias("src")
    .join(df_sessions.alias("b"), F.col("src.session_id") == F.col("b.id"), "left")
    .select("src.*", F.col("b.country"))
)

with_gaps = (
    with_dims
    .withColumn("is_new_burst", F.when(F.col("event_time") - F.lag("event_time").over(w7) > F.expr("INTERVAL 7 DAYS"), 1).otherwise(0))
)

sessions_web_events = with_gaps.withColumn("session_no", F.sum("is_new_burst").over(w8))

matched_keys = (
    sessions_web_events
    .select("page", "id")
    .intersect(
        df_web_events
        .filter(F.col("session_id").isin([8, 12, 18, 31]))
        .select("page", "id")
    )
)

kept_rows = (
    sessions_web_events
    .alias("s")
    .join(matched_keys.alias("k"), (F.col("s.page") == F.col("k.page")) & (F.col("s.id") == F.col("k.id")), "inner")
    .select("s.*")
)

result = (
    kept_rows
    .groupBy(
        "session_id",
        F.date_trunc("MONTH", "event_time").alias("activity_month"),
        F.date_trunc("YEAR", "event_time").alias("year_start"),
    )
    .agg(
        F.max("event_time").alias("ended_at"),
        F.count("*").alias("num_rows"),
    )
    .orderBy(F.col("session_id").desc_nulls_last())
)
