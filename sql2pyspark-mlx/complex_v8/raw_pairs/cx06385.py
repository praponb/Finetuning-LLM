from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_student_id = Window.partitionBy("student_id").orderBy("enrolled_at", "id")
w_student_id_running = Window.partitionBy("student_id").orderBy("enrolled_at", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_session_end_month_session_no = Window.partitionBy(F.month("session_end"), "session_no").orderBy(F.col("session_term_len").desc(), "student_id", "session_no")
w_student_id_2 = Window.partitionBy("student_id")
w_all_by_n_events = Window.orderBy("n_events", "student_id", "session_no")
w_all_by_duration_min_full = Window.orderBy(F.col("duration_min").desc(), "student_id", "session_no").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)

recent_enrollments = (
    df_enrollments
    .filter(F.col("grade_status").isin("WITHDRAWN", "INCOMPLETE"))
    .select(
        "id",
        "student_id",
        "course_id",
        "term",
        "score",
        "enrolled_at",
        F.length(F.trim(F.col("term"))).alias("term_len"),
    )
)

with_prev = recent_enrollments.withColumn("prev_enrolled_at", F.lag("enrolled_at").over(w_student_id))

flagged = (
    with_prev
    .withColumn("starts_session", F.when(F.col("prev_enrolled_at").isNull() | ((F.unix_timestamp("enrolled_at") - F.unix_timestamp("prev_enrolled_at")) / 60 > 30), 1).otherwise(0))
)

sessionized = flagged.withColumn("session_no", F.sum("starts_session").over(w_student_id_running))

session_summary = (
    sessionized
    .groupBy("student_id", "session_no")
    .agg(
        F.min("enrolled_at").alias("session_start"),
        F.max("enrolled_at").alias("session_end"),
        F.count("*").alias("n_events"),
        F.sum("term_len").alias("session_term_len"),
        F.round((F.unix_timestamp(F.max("enrolled_at")) - F.unix_timestamp(F.min("enrolled_at"))) / 60, 1).alias("duration_min"),
    )
)

metrics = (
    session_summary
    .select(
        "*",
        F.round((F.unix_timestamp("session_end") - F.unix_timestamp(F.lag("session_end").over(w_session_end_month_session_no))) / 60, 1).alias("minutes_since_prev_session_end"),
        F.round(F.col("session_no") - F.avg("session_no").over(w_student_id_2), 2).alias("session_no_vs_student_id_avg"),
        F.row_number().over(w_all_by_n_events).alias("n_events_seq"),
        F.first("session_term_len").over(w_all_by_duration_min_full).alias("first_session_term_len"),
        F.sum("n_events").over(w_student_id_2).alias("student_id_sum_n_events"),
    )
)

result = (
    metrics
    .select(
        "student_id",
        "session_no",
        "session_start",
        "session_end",
        "n_events",
        "minutes_since_prev_session_end",
        "session_no_vs_student_id_avg",
        "first_session_term_len",
        F.datediff(F.lit("2025-06-30").cast("date"), F.col("session_end")).alias("days_since_session_end"),
        F.round(F.col("n_events") / F.nullif(F.col("n_events_seq"), F.lit(0)), 3).alias("n_events_per_n_events_seq"),
    )
    .orderBy(F.col("session_end").desc(), "student_id", "session_no")
    .limit(10)
)
