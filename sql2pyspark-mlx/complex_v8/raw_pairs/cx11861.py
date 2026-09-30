from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_student_id = Window.partitionBy("student_id").orderBy("enrolled_at", "id")
w_student_id_running = Window.partitionBy("student_id").orderBy("enrolled_at", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_all_by_session_end_ties = Window.orderBy("session_end")
w_event_count_by_session_start = Window.partitionBy("event_count").orderBy("session_start", "student_id", "session_no")
w_all_by_session_end_full = Window.orderBy("session_end", "student_id", "session_no").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)

base = (
    df_enrollments
    .filter(F.col("enrolled_at") >= F.lit("2025-01-25 13:00:00").cast("timestamp"))
    .select(
        "id",
        "student_id",
        "course_id",
        "term",
        "score",
        "enrolled_at",
        F.date_trunc("week", F.col("enrolled_at")).alias("enrolled_at_week"),
    )
)

with_prev = base.withColumn("prev_enrolled_at", F.lag("enrolled_at").over(w_student_id))

flagged = (
    with_prev
    .withColumn("new_session_flag", F.when(F.col("prev_enrolled_at").isNull() | ((F.unix_timestamp("enrolled_at") - F.unix_timestamp("prev_enrolled_at")) / 60 > 90), 1).otherwise(0))
)

sessionized = flagged.withColumn("session_no", F.sum("new_session_flag").over(w_student_id_running))

per_session = (
    sessionized
    .groupBy("student_id", "session_no")
    .agg(
        F.min("enrolled_at").alias("session_start"),
        F.max("enrolled_at").alias("session_end"),
        F.count("*").alias("event_count"),
        F.round((F.unix_timestamp(F.max("enrolled_at")) - F.unix_timestamp(F.min("enrolled_at"))) / 60, 1).alias("duration_min"),
    )
)

without_matches = (
    per_session.alias("s")
    .join(df_enrollments.alias("e"), F.col("e.student_id") == F.col("s.student_id"), "left_semi")
    .filter(F.to_date(F.col("session_start")) < F.lit("2025-02-21").cast("date"))
)

metrics = (
    without_matches
    .select(
        "student_id",
        "session_no",
        "session_start",
        "session_end",
        F.round(F.percent_rank().over(w_all_by_session_end_ties), 3).alias("session_end_percent_rank"),
        F.round(F.cume_dist().over(w_event_count_by_session_start), 3).alias("session_start_cume_dist"),
        F.first("session_end").over(w_all_by_session_end_full).alias("first_session_end"),
        F.dense_rank().over(w_event_count_by_session_start).alias("session_start_dense_rank"),
    )
)

result = (
    metrics
    .filter(F.col("session_no") < 5)
    .select(
        "student_id",
        "session_start",
        "first_session_end",
        "session_start_dense_rank",
    )
    .orderBy("first_session_end")
)
