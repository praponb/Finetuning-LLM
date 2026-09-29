from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_course_id = Window.partitionBy("course_id").orderBy("enrolled_at", "id")
w_course_id_running = Window.partitionBy("course_id").orderBy("enrolled_at", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_grade_status_by_enrolled_at = Window.partitionBy("grade_status").orderBy("enrolled_at", "id").rowsBetween(-1, Window.currentRow)
w_is_new_session_grade_status = Window.partitionBy("is_new_session", "grade_status").orderBy(F.col("is_new_session").desc(), "id").rowsBetween(-2, Window.currentRow)
w_all_by_score_filled_moving_3 = Window.orderBy(F.col("score_filled").asc_nulls_last(), "id").rowsBetween(-3, Window.currentRow)
w_student_id = Window.partitionBy("student_id")

cleaned = (
    df_enrollments
    .select(
        "id",
        "student_id",
        "course_id",
        "score",
        "grade_status",
        "enrolled_at",
        F.coalesce(F.col("score"), F.lit(0.0)).alias("score_filled"),
    )
)

with_prev = cleaned.withColumn("prev_enrolled_at", F.lag("enrolled_at").over(w_course_id))

flagged = (
    with_prev
    .withColumn("is_new_session", F.when(F.col("prev_enrolled_at").isNull() | (F.col("enrolled_at") > F.col("prev_enrolled_at") + F.expr("INTERVAL 60 MINUTES")), 1).otherwise(0))
)

sessionized = flagged.withColumn("session_no", F.sum("is_new_session").over(w_course_id_running))

with_windows = (
    sessionized
    .select(
        "*",
        F.round(F.avg("is_new_session").over(w_grade_status_by_enrolled_at), 2).alias("is_new_session_avg_2"),
        F.round(F.avg("is_new_session").over(w_is_new_session_grade_status), 2).alias("is_new_session_avg_3"),
        F.round(F.avg("session_no").over(w_all_by_score_filled_moving_3), 2).alias("session_no_avg_4"),
        F.count(F.lit(1)).over(w_student_id).alias("rows_in_student_id"),
        F.round(F.col("score") / F.nullif(F.sum("score").over(w_student_id), F.lit(0)), 4).alias("score_share_of_student_id"),
    )
)

derived = (
    with_windows
    .select(
        "*",
        F.round(F.col("session_no") / F.nullif(F.col("score"), F.lit(0)), 3).alias("session_no_per_score"),
    )
)

result = (
    derived
    .select(
        "id",
        "student_id",
        "course_id",
        "score",
        "enrolled_at",
        "prev_enrolled_at",
        "is_new_session",
        "session_no",
        "is_new_session_avg_2",
        "is_new_session_avg_3",
        "session_no_avg_4",
        "session_no_per_score",
    )
    .orderBy(F.col("session_no_per_score").asc_nulls_last(), "id")
    .limit(10)
)
