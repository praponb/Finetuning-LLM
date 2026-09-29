from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_grade_status_term_by_score_a = Window.partitionBy("grade_status", "term").orderBy(F.col("score_adj").desc(), "id")

valid = (
    df_enrollments
    .filter(F.col("student_id") >= 6)
    .select(
        "id",
        "student_id",
        "course_id",
        "term",
        "score",
        "grade_status",
        "enrolled_at",
        F.round(F.col("score") * 0.9, 2).alias("score_adj"),
    )
)

windowed = (
    valid
    .select(
        "id",
        "course_id",
        "term",
        "score",
        "grade_status",
        F.row_number().over(w_grade_status_term_by_score_a).alias("rn"),
        F.round(F.cume_dist().over(w_grade_status_term_by_score_a), 3).alias("score_adj_cume_dist"),
    )
)

detail_rows = (
    windowed.alias("s")
    .join(df_enrollments.alias("e"), F.col("e.course_id") == F.col("s.course_id"), "left")
    .select(
        "s.id",
        "s.course_id",
        "s.term",
        "s.score",
        "s.grade_status",
        "s.rn",
        "s.score_adj_cume_dist",
        F.col("e.id").alias("enrollment_id"),
        F.col("e.enrolled_at").alias("enrollment_enrolled_at"),
        F.col("e.term").alias("enrollment_term"),
    )
)

without_matches = (
    detail_rows.alias("s")
    .join(df_enrollments.alias("e"), (F.col("e.course_id") == F.col("s.course_id")) & (F.col("e.student_id") <= 19), "left_semi")
)

result = (
    without_matches
    .groupBy("term", "enrollment_term")
    .pivot("grade_status", ["INCOMPLETE", "PASS"])
    .agg(F.sum("score_adj_cume_dist").alias("total"), F.count("score_adj_cume_dist").alias("n"))
    .orderBy("term", "enrollment_term")
)
