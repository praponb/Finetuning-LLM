from pyspark.sql import functions as F

filtered_students = (
    df_students
    .filter(~F.col("major").isin("Art", "Bio"))
    .select(
        "id",
        "student_name",
        "cohort",
        "major",
        "enrolled_on",
        "gpa",
        "is_international",
    )
)

labeled = (
    filtered_students
    .filter(F.year(F.col("enrolled_on")) == 2024)
    .select(
        "id",
        "student_name",
        "cohort",
        "major",
        "enrolled_on",
        "is_international",
        F.date_trunc("week", F.col("enrolled_on")).alias("enrolled_on_week"),
        F.substring(F.trim(F.col("student_name")), 1, 2).alias("student_name_prefix"),
        F.date_format(F.col("enrolled_on"), "yyyy-MM").alias("enrolled_on_label"),
    )
)

exploded_detail_enrollments = (
    labeled.alias("s")
    .join(df_enrollments.alias("e"), F.col("e.student_id") == F.col("s.id"), "inner")
    .select(
        "s.id",
        "s.student_name",
        "s.cohort",
        F.col("e.id").alias("enrollment_id"),
        F.col("e.term").alias("enrollment_term"),
        F.col("e.grade_status").alias("enrollment_grade_status"),
    )
)

detail_rows = (
    exploded_detail_enrollments.alias("s")
    .join(df_enrollments.alias("e"), F.col("e.student_id") == F.col("s.id"), "left")
    .select(
        "s.id",
        "s.cohort",
        "s.enrollment_id",
        "s.enrollment_term",
        F.col("e.id").alias("enrollment_id_2"),
        F.col("e.term").alias("enrollment_term_2"),
        F.col("e.grade_status").alias("enrollment_grade_status_2"),
    )
)

result = (
    detail_rows
    .filter(F.col("enrollment_term_2") == "2024T2")
    .select(
        "id",
        "cohort",
        "enrollment_id",
        "enrollment_term",
        "enrollment_id_2",
        "enrollment_term_2",
        "enrollment_grade_status_2",
    )
    .orderBy(F.col("cohort").desc())
)
