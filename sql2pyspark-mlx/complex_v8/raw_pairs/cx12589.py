from pyspark.sql import functions as F

valid = (
    df_enrollments
    .filter((F.to_date(F.col("enrolled_at")) < F.lit("2025-03-08").cast("date")) & (F.col("term").isin("2024T2", "2025T2", "2025T1")))
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

with_details = (
    valid.alias("s")
    .join(df_courses.alias("c"), F.col("c.id") == F.col("s.course_id"), "inner")
    .filter(F.col("c.dept").isNotNull())
    .select(
        "s.id",
        "s.student_id",
        "s.course_id",
        "s.term",
        "s.grade_status",
        "c.course_code",
        F.col("c.dept").alias("course_dept"),
        F.col("c.level").alias("course_level"),
    )
)

labeled = (
    with_details
    .withColumn("course_dept_group", F.when(F.col("course_dept").isin("Bio", "CS"), "group_a").when(F.col("course_dept") == "Math", "group_b").otherwise("other"))
)

result = (
    labeled
    .select(
        "id",
        "student_id",
        "course_id",
        "grade_status",
        "course_dept",
        "course_level",
        "course_dept_group",
        F.lower(F.regexp_replace(F.trim(F.col("course_dept_group")), " ", "_")).alias("course_dept_group_slug"),
        F.coalesce(F.col("term"), F.lit("UNKNOWN")).alias("term_or_unknown"),
    )
    .orderBy(
        F.col("student_id").desc_nulls_first(),
        F.col("course_dept_group_slug").asc_nulls_last(),
    )
)
