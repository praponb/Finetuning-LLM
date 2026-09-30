from pyspark.sql import functions as F

base_students = (
    df_students
    .filter((F.col("enrolled_on").between(F.lit("2024-01-24").cast("date"), F.lit("2025-06-27").cast("date"))) & (~F.col("cohort").isin("C2022", "C2023")))
    .select(
        "id",
        "student_name",
        "major",
        "is_international",
        F.coalesce(F.col("gpa"), F.lit(0.0)).alias("gpa_filled"),
        F.when(F.col("cohort").isin("C2023", "C2022"), "group_a").when(F.col("cohort") == "C2024", "group_b").otherwise("other").alias("cohort_group"),
    )
)

detail_rows_enrollments = (
    base_students.alias("s")
    .join(df_enrollments.alias("e"), (F.col("e.student_id") == F.col("s.id")) & (~F.col("e.grade_status").isin("WITHDRAWN", "PASS")), "inner")
    .select(
        "s.id",
        "s.student_name",
        "s.is_international",
        "s.gpa_filled",
        "s.cohort_group",
        F.col("e.id").alias("enrollment_id"),
        F.col("e.course_id").alias("enrollment_course_id"),
        F.col("e.enrolled_at").alias("enrollment_enrolled_at"),
        F.col("e.term").alias("enrollment_term"),
        F.col("e.grade_status").alias("enrollment_grade_status"),
    )
)

exploded_detail = (
    detail_rows_enrollments.alias("s")
    .join(df_enrollments.alias("e"), F.col("e.student_id") == F.col("s.id"), "inner")
    .select(
        "s.id",
        "s.is_international",
        "s.gpa_filled",
        "s.cohort_group",
        "s.enrollment_id",
        "s.enrollment_course_id",
        "s.enrollment_enrolled_at",
        "s.enrollment_term",
        "s.enrollment_grade_status",
        F.col("e.id").alias("enrollment_id_2"),
        F.col("e.course_id").alias("enrollment_course_id_2"),
        F.col("e.term").alias("enrollment_term_2"),
        F.col("e.grade_status").alias("enrollment_grade_status_2"),
    )
)

annotated = (
    exploded_detail
    .select(
        "*",
        F.coalesce(F.col("enrollment_term"), F.lit("UNKNOWN")).alias("enrollment_term_or_unknown"),
    )
)

result = (
    annotated
    .filter(F.col("enrollment_course_id").isNotNull())
    .select(
        "id",
        "is_international",
        "gpa_filled",
        "cohort_group",
        "enrollment_id",
        "enrollment_course_id",
        "enrollment_grade_status",
        "enrollment_term_2",
        "enrollment_grade_status_2",
        "enrollment_term_or_unknown",
    )
    .orderBy(F.col("id").desc_nulls_first())
)
