from pyspark.sql import functions as F

scoped_students = (
    df_students
    .filter(F.upper(F.trim(F.col("student_name"))).like("C%"))
    .select(
        "id",
        "student_name",
        "cohort",
        "major",
        "enrolled_on",
        "gpa",
    )
)

scored = (
    scoped_students
    .withColumn("cohort_slug", F.lower(F.regexp_replace(F.trim(F.col("cohort")), " ", "_")))
)

filtered = (
    scored
    .crossJoin(scored.agg(F.min("gpa").alias("overall_min_gpa")))
    .filter(F.col("gpa") > F.col("overall_min_gpa"))
    .drop("overall_min_gpa")
)

line_level = (
    filtered.alias("s")
    .join(df_enrollments.alias("e"), F.col("e.student_id") == F.col("s.id"), "left")
    .select(
        "s.id",
        "s.student_name",
        "s.cohort",
        "s.enrolled_on",
        "s.gpa",
        F.col("e.id").alias("enrollment_id"),
        F.col("e.score").alias("enrollment_score"),
        F.col("e.grade_status").alias("enrollment_grade_status"),
        F.col("e.term").alias("enrollment_term"),
        F.col("e.enrolled_at").alias("enrollment_enrolled_at"),
    )
)

result = (
    line_level
    .select(
        "id",
        "enrolled_on",
        "enrollment_id",
        "enrollment_score",
        "enrollment_grade_status",
        "enrollment_enrolled_at",
    )
    .orderBy("enrolled_on", "id", "enrollment_id")
    .limit(5)
)
