from pyspark.sql import functions as F

cleaned_students = (
    df_students
    .filter(F.year(F.col("enrolled_on")) == 2024)
    .select(
        "id",
        "student_name",
        "cohort",
        "major",
        "enrolled_on",
        "gpa",
        "is_international",
        F.round(F.col("gpa") * 0.9, 2).alias("gpa_adj"),
        F.lower(F.regexp_replace(F.trim(F.col("major")), " ", "_")).alias("major_slug"),
    )
)

with_children = (
    cleaned_students.alias("s")
    .join(df_enrollments.alias("e"), (F.col("e.student_id") == F.col("s.id")) & (F.col("e.course_id") <= 6), "inner")
    .select(
        "s.id",
        "s.student_name",
        "s.major",
        "s.is_international",
        "s.gpa_adj",
        "s.major_slug",
        F.col("e.id").alias("enrollment_id"),
        F.col("e.grade_status").alias("enrollment_grade_status"),
        F.col("e.score").alias("enrollment_score"),
        F.col("e.course_id").alias("enrollment_course_id"),
        F.col("e.term").alias("enrollment_term"),
    )
)

with_details = (
    with_children.alias("s")
    .join(df_courses.alias("c"), (F.col("c.id") == F.col("s.enrollment_course_id")) & (F.col("c.credits") <= 3), "inner")
    .filter(F.col("c.credits").between(2, 4))
    .select(
        "s.id",
        "s.student_name",
        "s.is_international",
        "s.gpa_adj",
        "s.major_slug",
        "s.enrollment_id",
        "s.enrollment_score",
        "s.enrollment_course_id",
        "s.enrollment_term",
        F.col("c.dept").alias("course_dept"),
        F.col("c.credits").alias("course_credits"),
        "c.course_code",
    )
)

result = (
    with_details
    .select(
        "id",
        "enrollment_id",
        F.col("enrollment_score").alias("enrollment_score_v"),
        F.col("gpa_adj").alias("gpa_adj_v"),
    )
    .unpivot(
        ids=["id", "enrollment_id"],
        values=["enrollment_score_v", "gpa_adj_v"],
        variableColumnName="metric",
        valueColumnName="metric_value",
    )
    .filter(F.col("metric_value").isNotNull())
    .orderBy("id", "enrollment_id", "metric")
)
