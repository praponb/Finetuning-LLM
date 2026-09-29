from pyspark.sql import functions as F

base = (
    df_enrollments
    .filter((F.col("score") >= 32.7) & (F.to_date(F.col("enrolled_at")) < F.lit("2025-02-06").cast("date")))
    .select(
        "id",
        "student_id",
        "course_id",
        "term",
        "score",
        "grade_status",
        "enrolled_at",
        F.length(F.trim(F.col("grade_status"))).alias("grade_status_len"),
        F.concat_ws("|", F.col("grade_status"), F.col("term")).alias("grade_status_term_label"),
        F.upper(F.trim(F.col("grade_status"))).alias("grade_status_clean"),
    )
)

exploded_detail = (
    base.alias("s")
    .join(df_enrollments.alias("e"), (F.col("e.student_id") == F.col("s.student_id")) & (F.col("e.course_id") <= 5), "inner")
    .select(
        "s.id",
        "s.student_id",
        "s.course_id",
        "s.grade_status",
        "s.enrolled_at",
        "s.grade_status_len",
        F.col("e.id").alias("enrollment_id"),
        F.col("e.grade_status").alias("enrollment_grade_status"),
        F.col("e.term").alias("enrollment_term"),
        F.col("e.course_id").alias("enrollment_course_id"),
        F.col("e.enrolled_at").alias("enrollment_enrolled_at"),
    )
)

result = (
    exploded_detail
    .cube("enrollment_id", "student_id", "grade_status_len")
    .agg(
        F.min("enrollment_enrolled_at").alias("first_enrollment_enrolled_at"),
        F.count("*").alias("cnt"),
        F.sum("grade_status_len").alias("total_grade_status_len"),
    )
)
