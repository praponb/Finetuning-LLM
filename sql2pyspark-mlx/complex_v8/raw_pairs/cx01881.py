from pyspark.sql import functions as F

scoped_enrollments = (
    df_enrollments
    .filter(F.col("term").isin("2024T2", "2025T1"))
    .select(
        "id",
        "student_id",
        "course_id",
        "term",
        "grade_status",
        "enrolled_at",
    )
)

joined = (
    scoped_enrollments.alias("s")
    .join(df_students.alias("st"), F.col("st.id") == F.col("s.student_id"), "left")
    .join(df_courses.alias("c"), F.col("c.id") == F.col("s.course_id"), "inner")
    .select(
        "s.id",
        "s.student_id",
        "s.course_id",
        "s.grade_status",
        "s.enrolled_at",
        F.col("st.gpa").alias("student_gpa"),
        F.col("st.major").alias("student_major"),
        "c.course_code",
        F.col("c.level").alias("course_level"),
    )
)

grade_status_stats = (
    joined
    .groupBy("grade_status")
    .agg(F.avg("student_gpa").alias("avg_student_gpa"))
)

outliers = (
    joined.alias("s")
    .join(grade_status_stats.alias("g"), F.col("g.grade_status") == F.col("s.grade_status"), "inner")
    .filter(F.col("s.student_gpa") > F.col("g.avg_student_gpa"))
    .select(
        "s.*",
        "g.avg_student_gpa",
        F.round(F.col("s.student_gpa") - F.col("g.avg_student_gpa"), 2).alias("student_gpa_minus_avg"),
    )
)

result = (
    outliers
    .cube("course_id", "course_level", "student_major")
    .agg(F.grouping_id().alias("grouping_level"), F.count("*").alias("cnt"))
    .orderBy(
        F.col("course_id").asc_nulls_last(),
        F.col("course_level").asc_nulls_last(),
        F.col("student_major").asc_nulls_last(),
    )
)
