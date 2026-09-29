from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_grade_status = Window.partitionBy("grade_status").orderBy("enrolled_at", "id")
w_grade_status_running = Window.partitionBy("grade_status").orderBy("enrolled_at", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_all_by_score = Window.orderBy("score", "id")
w_all_by_score_running = Window.orderBy("score", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_session_no_student_id = Window.partitionBy("session_no", "student_id")

filtered_enrollments = (
    df_enrollments
    .select(
        "id",
        "student_id",
        "course_id",
        "score",
        "grade_status",
        "enrolled_at",
    )
)

with_prev = filtered_enrollments.withColumn("prev_enrolled_at", F.lag("enrolled_at").over(w_grade_status))

flagged = (
    with_prev
    .withColumn("starts_session", F.when(F.col("prev_enrolled_at").isNull() | ((F.unix_timestamp("enrolled_at") - F.unix_timestamp("prev_enrolled_at")) / 60 > 90), 1).otherwise(0))
)

sessionized = flagged.withColumn("session_no", F.sum("starts_session").over(w_grade_status_running))

ranked = (
    sessionized
    .select(
        "*",
        F.lead("score").over(w_all_by_score).alias("next_score"),
        F.first("starts_session", ignorenulls=True).over(w_all_by_score_running).alias("first_known_starts_session"),
        F.row_number().over(w_all_by_score).alias("seq_no"),
        F.sum("starts_session").over(w_all_by_score_running).alias("running_starts_session"),
        F.count(F.lit(1)).over(w_session_no_student_id).alias("rows_in_session_no_student_id"),
        F.size(F.collect_set("grade_status").over(w_session_no_student_id)).alias("distinct_grade_status_in_session_no_student_id"),
    )
)

with_activity = (
    ranked.alias("s")
    .join(df_enrollments.alias("e"), (F.col("e.course_id") == F.col("s.course_id")) & (F.month(F.col("e.enrolled_at")).isin(2, 3)) & (F.col("e.student_id") <= 5), "left_anti")
)

result = (
    with_activity
    .select(
        "id",
        "student_id",
        "course_id",
        "score",
        "prev_enrolled_at",
        "next_score",
        "first_known_starts_session",
        "running_starts_session",
        "rows_in_session_no_student_id",
        "distinct_grade_status_in_session_no_student_id",
    )
    .orderBy("id", F.col("course_id").asc_nulls_last())
    .limit(5)
)
