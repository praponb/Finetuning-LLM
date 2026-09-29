from pyspark.sql import functions as F

scoped = (
    df_enrollments
    .filter(F.col("course_id") >= 3)
    .select(
        "id",
        "student_id",
        "course_id",
        "term",
        "score",
        "grade_status",
        "enrolled_at",
        F.length(F.trim(F.col("term"))).alias("term_len"),
        (F.floor(F.col("score") / 25) * 25).alias("score_bucket"),
        F.upper(F.trim(F.col("term"))).alias("term_clean"),
    )
)

enrollment_stats = (
    df_enrollments
    .groupBy("course_id")
    .agg(
        F.min("score").alias("min_enrollment_score"),
        F.sum("score").alias("total_enrollment_score"),
    )
)

profiled = (
    scoped.alias("s")
    .join(enrollment_stats.alias("e"), F.col("e.course_id") == F.col("s.course_id"), "left")
    .select("s.*", "e.min_enrollment_score", "e.total_enrollment_score")
)

enriched = (
    profiled
    .select(
        "*",
        F.initcap(F.trim(F.col("grade_status"))).alias("grade_status_clean"),
        F.substring(F.trim(F.col("grade_status")), 1, 2).alias("grade_status_prefix"),
        F.greatest(F.col("min_enrollment_score"), F.col("score")).alias("greatest_min_enrollment_score_score"),
    )
)

enriched_2 = (
    enriched.alias("s")
    .join(df_courses.alias("c"), F.col("c.id") == F.col("s.course_id"), "inner")
    .select(
        "s.id",
        "s.student_id",
        "s.course_id",
        "s.score",
        "s.score_bucket",
        "s.term_clean",
        "s.min_enrollment_score",
        "s.grade_status_clean",
        "s.grade_status_prefix",
        "s.greatest_min_enrollment_score_score",
        F.col("c.level").alias("course_level"),
        "c.course_code",
        F.col("c.dept").alias("course_dept"),
    )
)

enrollment_stats_2 = (
    df_enrollments
    .groupBy("student_id")
    .agg(
        F.min("enrolled_at").alias("first_enrollment_enrolled_at"),
        F.sum("score").alias("total_enrollment_score"),
    )
)

summarized_rows = (
    enriched_2.alias("s")
    .join(enrollment_stats_2.alias("e"), F.col("e.student_id") == F.col("s.student_id"), "left")
    .select("s.*", "e.first_enrollment_enrolled_at", "e.total_enrollment_score")
)

enriched_course = (
    summarized_rows.alias("s")
    .join(df_courses.alias("c"), F.col("c.id") == F.col("s.course_id"), "inner")
    .join(df_students.alias("st"), F.col("st.id") == F.col("s.student_id"), "left")
    .select(
        "s.id",
        "s.student_id",
        "s.course_id",
        "s.score",
        "s.score_bucket",
        "s.term_clean",
        "s.grade_status_prefix",
        "s.greatest_min_enrollment_score_score",
        "s.course_level",
        "s.course_code",
        "s.course_dept",
        "s.total_enrollment_score",
        F.col("c.course_code").alias("course_course_code"),
        F.col("c.dept").alias("course_dept_2"),
        F.col("st.is_international").alias("student_is_international"),
    )
)

enrollment_stats_3 = (
    df_enrollments
    .groupBy("student_id")
    .agg(F.max("enrolled_at").alias("last_enrollment_enrolled_at"))
)

with_counts = (
    enriched_course.alias("s")
    .join(enrollment_stats_3.alias("e"), F.col("e.student_id") == F.col("s.student_id"), "left")
    .select("s.*", "e.last_enrollment_enrolled_at")
)

result = (
    with_counts
    .select("id", "greatest_min_enrollment_score_score", "score")
    .unpivot(
        ids=["id"],
        values=["greatest_min_enrollment_score_score", "score"],
        variableColumnName="metric_name",
        valueColumnName="reading",
    )
    .orderBy("id", "metric_name")
)
