from pyspark.sql import functions as F

filtered = (
    df_enrollments
    .filter(F.col("score") > 8.7)
    .select(
        "id",
        "student_id",
        "course_id",
        "term",
        "grade_status",
        "enrolled_at",
    )
)

with_parent = (
    filtered.alias("s")
    .join(df_courses.alias("c"), F.col("c.id") == F.col("s.course_id"), "left")
    .select(
        "s.id",
        "s.student_id",
        "s.course_id",
        "s.term",
        "s.grade_status",
        "s.enrolled_at",
        "c.course_code",
        F.col("c.level").alias("course_level"),
    )
)

scored = (
    with_parent
    .select(
        "id",
        "student_id",
        "course_id",
        "grade_status",
        F.lower(F.trim(F.col("course_code"))).alias("course_code_clean"),
    )
)

result = (
    scored
    .groupingSets(
        [["grade_status", "course_id", "student_id"], ["grade_status"], []],
        "grade_status", "course_id", "student_id",
    )
    .agg(F.countDistinct("course_id").alias("distinct_course_ids"))
    .select(
        F.coalesce(F.col("grade_status"), F.lit("ALL")).alias("grade_status"),
        "course_id",
        "student_id",
        "distinct_course_ids",
    )
    .orderBy(
        F.col("grade_status").asc_nulls_last(),
        F.col("course_id").asc_nulls_last(),
        F.col("student_id").asc_nulls_last(),
    )
)
