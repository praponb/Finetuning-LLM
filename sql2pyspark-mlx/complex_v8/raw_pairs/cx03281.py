from pyspark.sql import functions as F

filtered_enrollments = (
    df_enrollments
    .filter((~F.col("grade_status").isin("PASS", "INCOMPLETE")) & (F.col("course_id") <= 2))
    .select(
        "id",
        "student_id",
        "course_id",
        "term",
        "score",
        "grade_status",
    )
)

enriched = (
    filtered_enrollments.alias("s")
    .join(df_courses.alias("c"), F.col("c.id") == F.col("s.course_id"), "inner")
    .join(df_students.alias("st"), F.col("st.id") == F.col("s.student_id"), "left")
    .select(
        "s.id",
        "s.student_id",
        "s.course_id",
        "s.score",
        F.col("c.dept").alias("course_dept"),
        F.col("c.level").alias("course_level"),
        "c.course_code",
        F.col("st.cohort").alias("student_cohort"),
        F.col("st.major").alias("student_major"),
    )
)

enrollment_stats = (
    df_enrollments
    .groupBy("student_id")
    .agg(
        F.sum("score").alias("total_enrollment_score"),
        F.count("*").alias("enrollment_count"),
    )
)

summarized_rows = (
    enriched.alias("s")
    .join(enrollment_stats.alias("e"), F.col("e.student_id") == F.col("s.student_id"), "left")
    .select(
        "s.*",
        "e.total_enrollment_score",
        F.coalesce(F.col("e.enrollment_count"), F.lit(0)).alias("enrollment_count"),
    )
)

result = (
    summarized_rows
    .groupBy("enrollment_count")
    .pivot("course_dept", ["Math", "Art", "Bio", "Biz"])
    .agg(F.max("score"))
)
