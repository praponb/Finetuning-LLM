from pyspark.sql import functions as F

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
        F.round(F.col("score") * 1.07, 2).alias("score_adj"),
        F.concat_ws(": ", F.col("term"), F.col("grade_status")).alias("term_grade_status_label"),
    )
)

exploded_detail_enrollments = (
    scoped_enrollments.alias("s")
    .join(df_enrollments.alias("e"), F.col("e.student_id") == F.col("s.student_id"), "inner")
    .select(
        "s.id",
        "s.student_id",
        "s.course_id",
        "s.score",
        "s.grade_status",
        "s.score_adj",
        F.col("e.id").alias("enrollment_id"),
        F.col("e.grade_status").alias("enrollment_grade_status"),
        F.col("e.enrolled_at").alias("enrollment_enrolled_at"),
        F.col("e.course_id").alias("enrollment_course_id"),
        F.col("e.score").alias("enrollment_score"),
    )
)

result = (
    exploded_detail_enrollments
    .groupBy("course_id", "enrollment_course_id")
    .pivot("enrollment_grade_status", ["PASS", "WITHDRAWN", "INCOMPLETE", "FAIL"])
    .agg(F.sum("enrollment_score"))
    .orderBy("course_id", "enrollment_course_id")
)
