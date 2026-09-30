from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_grade_status_by_enrolled_at = Window.partitionBy("grade_status").orderBy("enrolled_at", "id").rowsBetween(-3, -1)
w_course_id_by_score_ties = Window.partitionBy("course_id").orderBy(F.col("score").desc_nulls_first())

prepared_enrollments = (
    df_enrollments
    .filter((F.col("term") == "2024T2") & (F.col("student_id").isNotNull()))
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

with_windows = (
    prepared_enrollments
    .select(
        "id",
        "student_id",
        "course_id",
        "term",
        "score",
        "grade_status",
        F.round(F.avg("score").over(w_grade_status_by_enrolled_at), 2).alias("score_avg_3_prior"),
        F.dense_rank().over(w_course_id_by_score_ties).alias("score_dense_rank"),
    )
)

with_children = (
    with_windows.alias("s")
    .join(df_enrollments.alias("e"), F.col("e.course_id") == F.col("s.course_id"), "inner")
    .select(
        "s.id",
        "s.student_id",
        "s.course_id",
        "s.term",
        "s.score",
        "s.grade_status",
        F.col("e.id").alias("enrollment_id"),
        F.col("e.term").alias("enrollment_term"),
        F.col("e.grade_status").alias("enrollment_grade_status"),
        F.col("e.student_id").alias("enrollment_student_id"),
    )
)

result = (
    with_children
    .select(
        "id",
        "student_id",
        "term",
        "grade_status",
        "enrollment_term",
        "enrollment_grade_status",
    )
    .orderBy("grade_status", "term")
)
