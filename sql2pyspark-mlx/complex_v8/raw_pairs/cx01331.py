from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_course_id = Window.partitionBy("course_id").orderBy("enrolled_at", "id")
w_course_id_running = Window.partitionBy("course_id").orderBy("enrolled_at", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_grade_status_session_id_by_e = Window.partitionBy("grade_status", "session_id").orderBy("enrolled_at", "id")
w_session_id_is_new_session_by = Window.partitionBy("session_id", "is_new_session").orderBy("session_id", "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_session_id_is_new_session_by_2 = Window.partitionBy("session_id", "is_new_session").orderBy("session_id", "id")
w_grade_status_session_id_by_e_2 = Window.partitionBy("grade_status", "session_id").orderBy("enrolled_at")
w_student_id_by_session_id_ful = Window.partitionBy("student_id").orderBy(F.col("session_id").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)

cleaned_enrollments = (
    df_enrollments
    .filter(F.col("grade_status") == "FAIL")
    .select(
        "id",
        "student_id",
        "course_id",
        "term",
        "score",
        "grade_status",
        "enrolled_at",
        F.concat_ws("|", F.col("grade_status"), F.col("term")).alias("grade_status_term_label"),
    )
)

with_prev = cleaned_enrollments.withColumn("prev_enrolled_at", F.lag("enrolled_at").over(w_course_id))

flagged = (
    with_prev
    .withColumn("is_new_session", F.when(F.col("prev_enrolled_at").isNull() | (F.col("enrolled_at") > F.col("prev_enrolled_at") + F.expr("INTERVAL 45 MINUTES")), 1).otherwise(0))
)

sessionized = flagged.withColumn("session_id", F.sum("is_new_session").over(w_course_id_running))

screened = (
    sessionized.alias("s")
    .join(df_enrollments.alias("e"), F.col("e.course_id") == F.col("s.course_id"), "left_semi")
)

with_trends = (
    screened
    .select(
        "id",
        "term",
        "grade_status",
        "enrolled_at",
        "grade_status_term_label",
        "is_new_session",
        "session_id",
        F.ntile(5).over(w_grade_status_session_id_by_e).alias("enrolled_at_quintile"),
        F.nth_value("score", 2).over(w_session_id_is_new_session_by).alias("second_score"),
        F.ntile(3).over(w_session_id_is_new_session_by_2).alias("session_id_tercile"),
        F.rank().over(w_grade_status_session_id_by_e_2).alias("enrolled_at_rank"),
        F.nth_value("enrolled_at", 2).over(w_student_id_by_session_id_ful).alias("second_enrolled_at"),
        F.rank().over(w_grade_status_session_id_by_e).alias("enrolled_at_rank_2"),
    )
)

result = (
    with_trends
    .select(
        "id",
        "term",
        "grade_status",
        "grade_status_term_label",
        "session_id",
        "enrolled_at_quintile",
        "second_score",
        "session_id_tercile",
        "enrolled_at_rank",
        "second_enrolled_at",
        "enrolled_at_rank_2",
        (F.floor(F.col("second_score") / 25) * 25).alias("second_score_bucket"),
        F.concat_ws("|", F.col("term"), F.col("grade_status_term_label")).alias("term_grade_status_term_label_label"),
    )
    .orderBy("term_grade_status_term_label_label", F.col("session_id").desc())
)
