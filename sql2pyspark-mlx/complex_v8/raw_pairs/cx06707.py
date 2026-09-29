from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_grade_status = Window.partitionBy("grade_status").orderBy("enrolled_at", "id")
w_grade_status_running = Window.partitionBy("grade_status").orderBy("enrolled_at", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_session_end_to_date_event_co = Window.partitionBy(F.to_date("session_end"), "event_count").orderBy("duration_min", "grade_status", "session_id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_event_count = Window.partitionBy("event_count")
w_event_count_by_session_end = Window.partitionBy("event_count").orderBy(F.col("session_end").desc(), "grade_status", "session_id")
w_event_count_by_session_end_r = Window.partitionBy("event_count").orderBy(F.col("session_end").desc(), "grade_status", "session_id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

filtered_enrollments = df_enrollments

with_prev = filtered_enrollments.withColumn("prev_enrolled_at", F.lag("enrolled_at").over(w_grade_status))

flagged = (
    with_prev
    .withColumn("is_new_session", F.when(F.col("prev_enrolled_at").isNull() | ((F.unix_timestamp("enrolled_at") - F.unix_timestamp("prev_enrolled_at")) / 60 > 120), 1).otherwise(0))
)

sessionized = flagged.withColumn("session_id", F.sum("is_new_session").over(w_grade_status_running))

sessions = (
    sessionized
    .groupBy("grade_status", "session_id")
    .agg(
        F.min("enrolled_at").alias("session_start"),
        F.max("enrolled_at").alias("session_end"),
        F.count("*").alias("event_count"),
        F.sum("score").alias("session_score"),
        F.round((F.unix_timestamp(F.max("enrolled_at")) - F.unix_timestamp(F.min("enrolled_at"))) / 60, 1).alias("duration_min"),
    )
)

windowed = (
    sessions
    .select(
        "*",
        F.last("session_score").over(w_session_end_to_date_event_co).alias("last_session_score"),
        F.round(F.col("event_count") / F.nullif(F.sum("event_count").over(w_event_count), F.lit(0)), 4).alias("event_count_share_of_event_count"),
        F.count(F.lit(1)).over(w_event_count).alias("rows_in_event_count"),
        F.lead("session_score", 3).over(w_event_count_by_session_end).alias("next3_session_score"),
        F.last("event_count", ignorenulls=True).over(w_event_count_by_session_end_r).alias("last_known_event_count"),
    )
)

grouped = (
    windowed
    .filter(F.month(F.col("session_end")).isin(1, 3))
    .groupBy(
        "last_known_event_count",
        F.date_trunc("month", "session_end").alias("session_end_month"),
    )
    .agg(
        F.max("session_start").alias("last_session_start"),
        F.round(F.sum("last_session_score") / F.nullif(F.sum("event_count_share_of_event_count"), F.lit(0)), 3).alias("last_session_score_to_event_count_share_of_event"),
        F.count("*").alias("n_rows"),
        F.round(F.stddev("event_count_share_of_event_count"), 2).alias("stddev_event_count_share_of_event_count"),
    )
)

result = (
    grouped
    .select(
        "last_known_event_count",
        "session_end_month",
        "last_session_start",
        "last_session_score_to_event_count_share_of_event",
        "n_rows",
        "stddev_event_count_share_of_event_count",
        F.when(F.col("n_rows") >= 10, "large").when(F.col("n_rows") >= 4, "medium").otherwise("small").alias("n_rows_tier"),
    )
    .orderBy(
        "last_session_score_to_event_count_share_of_event",
        F.col("stddev_event_count_share_of_event_count").asc_nulls_last(),
        "last_known_event_count",
        "session_end_month",
    )
    .limit(20)
)
