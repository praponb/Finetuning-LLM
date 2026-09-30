from pyspark.sql import functions as F

recent_students = (
    df_students
    .select(
        "id",
        "student_name",
        "enrolled_on",
        "gpa",
        "is_international",
    )
)

screened = (
    recent_students
    .crossJoin(recent_students.agg(F.min("gpa").alias("overall_min_gpa")))
    .filter(F.col("gpa") > F.col("overall_min_gpa"))
    .drop("overall_min_gpa")
)

labeled = (
    screened
    .filter(F.col("enrolled_on").between(F.lit("2024-04-05").cast("date"), F.lit("2025-05-07").cast("date")))
    .withColumn("enrolled_on_label", F.date_format(F.col("enrolled_on"), "yyyy-MM-dd"))
)

exploded_detail_enrollments = (
    labeled.alias("s")
    .join(df_enrollments.alias("e"), F.col("e.student_id") == F.col("s.id"), "inner")
    .select(
        "s.id",
        "s.enrolled_on",
        "s.enrolled_on_label",
        F.col("e.id").alias("enrollment_id"),
        F.col("e.term").alias("enrollment_term"),
        F.col("e.enrolled_at").alias("enrollment_enrolled_at"),
        F.col("e.course_id").alias("enrollment_course_id"),
    )
)

joined_enrollment = (
    exploded_detail_enrollments.alias("s")
    .join(df_enrollments.alias("e"), F.col("e.id") == F.col("s.enrollment_id"), "left")
    .select(
        "s.id",
        "s.enrollment_id",
        "s.enrollment_enrolled_at",
        "s.enrollment_course_id",
        F.col("e.student_id").alias("enrollment_student_id"),
    )
)

with_parent = (
    joined_enrollment.alias("s")
    .join(df_courses.alias("c"), F.col("c.id") == F.col("s.enrollment_course_id"), "left")
    .select(
        "s.id",
        "s.enrollment_id",
        "s.enrollment_enrolled_at",
        "s.enrollment_course_id",
        "s.enrollment_student_id",
        F.col("c.level").alias("course_level"),
        F.col("c.credits").alias("course_credits"),
        F.col("c.dept").alias("course_dept"),
    )
)

result = (
    with_parent
    .select(
        "id",
        "enrollment_id",
        "enrollment_enrolled_at",
        "enrollment_course_id",
        "enrollment_student_id",
        "course_level",
        "course_dept",
    )
    .orderBy("enrollment_id")
)
