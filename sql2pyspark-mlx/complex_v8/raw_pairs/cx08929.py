from pyspark.sql import functions as F

cleaned = (
    df_enrollments
    .filter((F.col("grade_status").isNotNull()) & (F.col("score") < 78.8))
    .select(
        "id",
        "student_id",
        "course_id",
        "term",
        "grade_status",
        "enrolled_at",
        (F.floor(F.col("score") / 25) * 25).alias("score_bucket"),
    )
)

combined_details_student = (
    cleaned.alias("s")
    .join(df_students.alias("st"), F.col("st.id") == F.col("s.student_id"), "left")
    .select(
        "s.*",
        F.col("st.cohort").alias("student_cohort"),
        F.col("st.enrolled_on").alias("student_enrolled_on"),
        F.col("st.gpa").alias("student_gpa"),
    )
)

per_group = (
    combined_details_student
    .filter(F.col("student_cohort") == "C2021")
    .groupBy("course_id")
    .agg(
        F.round(F.sum("student_gpa") / F.nullif(F.sum("score_bucket"), F.lit(0)), 3).alias("student_gpa_to_score_bucket_ratio"),
        F.count("*").alias("n_rows"),
        F.round(100 * F.count(F.when(F.col("term") == "2025T2", 1)) / F.count("*"), 1).alias("pct_term_2025t2"),
        F.countDistinct("student_id").alias("distinct_student_ids"),
        F.min("enrolled_at").alias("first_enrolled_at"),
        F.avg("score_bucket").alias("avg_score_bucket_check"),
    )
    .filter(F.col("avg_score_bucket_check") > 30)
    .drop("avg_score_bucket_check")
)

with_parent = (
    per_group.alias("s")
    .join(df_courses.alias("c"), F.col("c.id") == F.col("s.course_id"), "inner")
    .filter(F.col("c.level").isin("intro", "core"))
    .select(
        "s.course_id",
        "s.student_gpa_to_score_bucket_ratio",
        "s.n_rows",
        "s.pct_term_2025t2",
        "s.distinct_student_ids",
        "s.first_enrolled_at",
        "c.course_code",
        F.col("c.level").alias("course_level"),
        F.col("c.dept").alias("course_dept"),
    )
)

result = (
    with_parent
    .groupBy("first_enrolled_at")
    .pivot("course_level", ["core", "advanced", "intro"])
    .agg(F.avg("student_gpa_to_score_bucket_ratio"))
    .orderBy("first_enrolled_at")
)
