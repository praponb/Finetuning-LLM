from pyspark.sql import functions as F

filtered_enrollments = (
    df_enrollments
    .filter(F.col("student_id") >= 16)
    .select(
        "id",
        "student_id",
        "course_id",
        "term",
        "score",
        "grade_status",
        F.when(F.col("term").isin("2025T2", "2025T1"), "group_a").when(F.col("term") == "2024T2", "group_b").otherwise("other").alias("term_group"),
        F.initcap(F.trim(F.col("grade_status"))).alias("grade_status_clean"),
        F.upper(F.trim(F.col("term"))).alias("term_clean"),
    )
)

joined = (
    filtered_enrollments.alias("s")
    .join(df_courses.alias("c"), F.col("c.id") == F.col("s.course_id"), "inner")
    .select(
        "s.id",
        "s.student_id",
        "s.course_id",
        "s.term",
        "s.score",
        "s.grade_status",
        "s.term_group",
        "s.grade_status_clean",
        "s.term_clean",
        F.col("c.dept").alias("course_dept"),
        "c.course_code",
    )
)

result = (
    joined
    .cube("course_dept", "grade_status")
    .agg(
        F.grouping_id().alias("grouping_level"),
        F.countDistinct("id").alias("distinct_ids"),
    )
    .orderBy(
        F.col("course_dept").asc_nulls_last(),
        F.col("grade_status").asc_nulls_last(),
    )
)
