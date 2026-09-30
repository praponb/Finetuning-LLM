from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_grade_status = Window.partitionBy("grade_status").orderBy("enrolled_at", "id")
w_grade_status_running = Window.partitionBy("grade_status").orderBy("enrolled_at", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_all_by_duration_min_running = Window.orderBy(F.col("duration_min").asc_nulls_last(), "grade_status", "burst_id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_all_by_duration_min_full = Window.orderBy(F.col("duration_min").asc_nulls_last(), "grade_status", "burst_id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_all_by_duration_min = Window.orderBy(F.col("duration_min").asc_nulls_last(), "grade_status", "burst_id")

base_enrollments = (
    df_enrollments
    .select(
        "id",
        "student_id",
        "course_id",
        "term",
        "score",
        "grade_status",
        "enrolled_at",
        F.lower(F.regexp_replace(F.trim(F.col("grade_status")), " ", "_")).alias("grade_status_slug"),
    )
)

with_prev = base_enrollments.withColumn("prev_enrolled_at", F.lag("enrolled_at").over(w_grade_status))

flagged = (
    with_prev
    .withColumn("starts_session", F.when(F.col("prev_enrolled_at").isNull() | ((F.unix_timestamp("enrolled_at") - F.unix_timestamp("prev_enrolled_at")) / 60 > 60), 1).otherwise(0))
)

sessionized = flagged.withColumn("burst_id", F.sum("starts_session").over(w_grade_status_running))

sessions = (
    sessionized
    .groupBy("grade_status", "burst_id")
    .agg(
        F.min("enrolled_at").alias("session_start"),
        F.max("enrolled_at").alias("session_end"),
        F.count("*").alias("n_events"),
        F.sum("score").alias("session_score"),
        F.round((F.unix_timestamp(F.max("enrolled_at")) - F.unix_timestamp(F.min("enrolled_at"))) / 60, 1).alias("duration_min"),
    )
)

metrics = (
    sessions
    .select(
        "grade_status",
        "burst_id",
        "session_end",
        "n_events",
        "session_score",
        "duration_min",
        F.first("burst_id", ignorenulls=True).over(w_all_by_duration_min_running).alias("first_known_burst_id"),
        F.nth_value("n_events", 2).over(w_all_by_duration_min_full).alias("second_n_events"),
        F.nth_value("session_score", 2).over(w_all_by_duration_min_full).alias("second_session_score"),
        (F.col("duration_min") - F.lag("duration_min").over(w_all_by_duration_min)).alias("duration_min_change"),
    )
)

result = (
    metrics
    .filter(F.col("session_score") >= 120.0)
    .select(
        "grade_status",
        "burst_id",
        "n_events",
        "session_score",
        "duration_min",
        "first_known_burst_id",
        "second_session_score",
        "duration_min_change",
        F.round(F.col("duration_min_change") * 1.1, 2).alias("duration_min_change_adj"),
        F.datediff(F.lit("2026-01-01").cast("date"), F.col("session_end")).alias("days_since_session_end"),
    )
)
