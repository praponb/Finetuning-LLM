from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_course_id = Window.partitionBy("course_id").orderBy("enrolled_at", "id")
w_course_id_running = Window.partitionBy("course_id").orderBy("enrolled_at", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_new_session_flag_by_score = Window.partitionBy("new_session_flag").orderBy("score", "id")
w_grade_status = Window.partitionBy("grade_status")

scoped_enrollments = (
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

with_prev = scoped_enrollments.withColumn("prev_enrolled_at", F.lag("enrolled_at").over(w_course_id))

flagged = (
    with_prev
    .withColumn("new_session_flag", F.when(F.col("prev_enrolled_at").isNull() | (F.col("enrolled_at") > F.col("prev_enrolled_at") + F.expr("INTERVAL 45 MINUTES")), 1).otherwise(0))
)

sessionized = flagged.withColumn("burst_id", F.sum("new_session_flag").over(w_course_id_running))

sequenced = (
    sessionized
    .select(
        "id",
        "student_id",
        "term",
        "score",
        "grade_status",
        "enrolled_at",
        "burst_id",
        F.row_number().over(w_new_session_flag_by_score).alias("score_seq"),
        F.round(F.col("new_session_flag") / F.nullif(F.sum("new_session_flag").over(w_grade_status), F.lit(0)), 4).alias("new_session_flag_share_of_grade_status"),
    )
)

result = (
    sequenced
    .filter(F.col("enrolled_at") < F.lit("2025-03-02 08:00:00").cast("timestamp"))
    .select(
        "id",
        "student_id",
        "term",
        "score",
        "grade_status",
        "enrolled_at",
        "score_seq",
        "new_session_flag_share_of_grade_status",
    )
    .orderBy(F.col("id").desc(), "score_seq")
)
