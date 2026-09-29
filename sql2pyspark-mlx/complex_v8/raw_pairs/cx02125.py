from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_grade_status = Window.partitionBy("grade_status").orderBy("enrolled_at", "id")
w_grade_status_running = Window.partitionBy("grade_status").orderBy("enrolled_at", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_all_by_is_new_session_full = Window.orderBy(F.col("is_new_session").asc_nulls_last(), "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_session_id_is_new_session = Window.partitionBy("session_id", "is_new_session")
w_all_by_is_new_session_moving = Window.orderBy(F.col("is_new_session").asc_nulls_last(), "id").rowsBetween(-1, Window.currentRow)

base = (
    df_enrollments
    .select(
        "id",
        "student_id",
        "course_id",
        "term",
        "score",
        "grade_status",
        "enrolled_at",
    )
)

with_prev = base.withColumn("prev_enrolled_at", F.lag("enrolled_at").over(w_grade_status))

flagged = (
    with_prev
    .withColumn("is_new_session", F.when(F.col("prev_enrolled_at").isNull() | (F.col("enrolled_at") > F.col("prev_enrolled_at") + F.expr("INTERVAL 30 MINUTES")), 1).otherwise(0))
)

sessionized = flagged.withColumn("session_id", F.sum("is_new_session").over(w_grade_status_running))

without_matches = (
    sessionized.alias("s")
    .join(df_enrollments.alias("e"), (F.col("e.student_id") == F.col("s.student_id")) & (F.col("e.course_id") >= 8) & (F.col("e.grade_status").isNotNull()), "left_semi")
)

metrics = (
    without_matches
    .select(
        "id",
        "course_id",
        "term",
        "score",
        "grade_status",
        "enrolled_at",
        "is_new_session",
        "session_id",
        F.first("prev_enrolled_at").over(w_all_by_is_new_session_full).alias("first_prev_enrolled_at"),
        F.count(F.lit(1)).over(w_session_id_is_new_session).alias("rows_in_session_id_is_new_session"),
        F.round(F.col("session_id") - F.avg("session_id").over(w_session_id_is_new_session), 2).alias("session_id_vs_session_id_is_new_session_avg"),
        F.sum("score").over(w_all_by_is_new_session_moving).alias("score_sum_2"),
    )
)

result = (
    metrics
    .groupBy("first_prev_enrolled_at")
    .pivot("grade_status", ["WITHDRAWN", "FAIL", "PASS", "INCOMPLETE"])
    .agg(F.avg("score"))
    .orderBy("first_prev_enrolled_at")
)
