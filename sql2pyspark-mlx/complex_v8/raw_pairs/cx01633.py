from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_term = Window.partitionBy("term").orderBy("enrolled_at", "id")
w_term_running = Window.partitionBy("term").orderBy("enrolled_at", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_events_by_session_end = Window.partitionBy("events").orderBy("session_end", "term", "burst_id")
w_burst_id_term = Window.partitionBy("burst_id", "term")
w_events_by_session_end_ties = Window.partitionBy("events").orderBy(F.col("session_end").desc_nulls_first())
w_events_by_session_end_2 = Window.partitionBy("events").orderBy(F.col("session_end").desc_nulls_first(), "term", "burst_id")

valid_enrollments = (
    df_enrollments
    .select(
        "id",
        "student_id",
        "course_id",
        "term",
        "score",
        "enrolled_at",
    )
)

with_prev = valid_enrollments.withColumn("prev_enrolled_at", F.lag("enrolled_at").over(w_term))

flagged = (
    with_prev
    .withColumn("starts_session", F.when(F.col("prev_enrolled_at").isNull() | ((F.unix_timestamp("enrolled_at") - F.unix_timestamp("prev_enrolled_at")) / 60 > 90), 1).otherwise(0))
)

sessionized = flagged.withColumn("burst_id", F.sum("starts_session").over(w_term_running))

session_summary = (
    sessionized
    .groupBy("term", "burst_id")
    .agg(
        F.min("enrolled_at").alias("session_start"),
        F.max("enrolled_at").alias("session_end"),
        F.count("*").alias("events"),
        F.round((F.unix_timestamp(F.max("enrolled_at")) - F.unix_timestamp(F.min("enrolled_at"))) / 60, 1).alias("duration_min"),
    )
)

metrics = (
    session_summary
    .select(
        "*",
        F.lead("session_start", 3).over(w_events_by_session_end).alias("next3_session_start"),
        F.round(F.avg("duration_min").over(w_burst_id_term), 2).alias("burst_id_term_avg_duration_min"),
        F.count(F.lit(1)).over(w_burst_id_term).alias("rows_in_burst_id_term"),
        F.round(F.col("burst_id") / F.nullif(F.sum("burst_id").over(w_burst_id_term), F.lit(0)), 4).alias("burst_id_share_of_burst_id_term"),
        F.dense_rank().over(w_events_by_session_end_ties).alias("session_end_dense_rank"),
        F.lag("events").over(w_events_by_session_end_2).alias("prev_events"),
    )
)

scored = (
    metrics
    .select(
        "*",
        F.coalesce(F.col("duration_min"), F.lit(0.0)).alias("duration_min_filled"),
        F.lower(F.regexp_replace(F.trim(F.col("term")), " ", "_")).alias("term_slug"),
    )
)

result = (
    scored
    .select(
        "term",
        "session_start",
        "session_end",
        "events",
        "duration_min",
        "next3_session_start",
        "burst_id_term_avg_duration_min",
        "rows_in_burst_id_term",
        "burst_id_share_of_burst_id_term",
        "session_end_dense_rank",
        "prev_events",
        "duration_min_filled",
        F.round(F.col("session_end_dense_rank") / F.nullif(F.col("rows_in_burst_id_term"), F.lit(0)), 3).alias("session_end_dense_rank_per_rows_in_burst_id_term"),
    )
)
