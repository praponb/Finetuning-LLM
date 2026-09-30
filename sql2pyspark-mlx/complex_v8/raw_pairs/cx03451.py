from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_course_id_grade_status_by_sc = Window.partitionBy("course_id", "grade_status").orderBy("score")
w_course_id_grade_status_by_sc_2 = Window.partitionBy("course_id", "grade_status").orderBy("score", "id").rowsBetween(-1, Window.currentRow)

filtered = (
    df_enrollments
    .filter((F.col("course_id") <= 3) & (F.col("grade_status").isNotNull()))
    .select(
        "id",
        "student_id",
        "course_id",
        "term",
        "score",
        "grade_status",
    )
)

windowed = (
    filtered
    .select(
        "id",
        "student_id",
        "course_id",
        "grade_status",
        F.round(F.percent_rank().over(w_course_id_grade_status_by_sc), 3).alias("score_percent_rank"),
        F.sum("score").over(w_course_id_grade_status_by_sc_2).alias("score_sum_2"),
    )
)

with_children = (
    windowed.alias("s")
    .join(df_enrollments.alias("e"), (F.col("e.course_id") == F.col("s.course_id")) & (F.hour(F.col("e.enrolled_at")).between(8, 14)), "inner")
    .select(
        "s.id",
        "s.student_id",
        "s.course_id",
        "s.grade_status",
        "s.score_percent_rank",
        "s.score_sum_2",
        F.col("e.id").alias("enrollment_id"),
        F.col("e.grade_status").alias("enrollment_grade_status"),
        F.col("e.enrolled_at").alias("enrollment_enrolled_at"),
    )
)

enriched = (
    with_children.alias("s")
    .join(df_enrollments.alias("e"), F.col("e.id") == F.col("s.enrollment_id"), "inner")
    .filter(F.col("e.term").isNotNull())
    .select(
        "s.id",
        "s.student_id",
        "s.course_id",
        "s.grade_status",
        "s.enrollment_id",
        "s.enrollment_grade_status",
        "s.enrollment_enrolled_at",
        F.col("e.term").alias("enrollment_term"),
        F.col("e.enrolled_at").alias("enrollment_enrolled_at_2"),
        F.col("e.score").alias("enrollment_score"),
    )
)

result = (
    enriched
    .select(
        "id",
        "student_id",
        "grade_status",
        "enrollment_id",
        "enrollment_term",
        "enrollment_score",
        (F.floor(F.col("enrollment_score") / 25) * 25).alias("enrollment_score_bucket"),
        F.initcap(F.trim(F.col("enrollment_term"))).alias("enrollment_term_clean"),
    )
    .orderBy(F.col("id").desc())
)
