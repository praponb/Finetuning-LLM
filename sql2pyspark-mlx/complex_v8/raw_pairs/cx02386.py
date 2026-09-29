from pyspark.sql import functions as F

cleaned_enrollments = (
    df_enrollments
    .filter((F.col("student_id") >= 10) & (F.col("course_id") >= 6))
    .select(
        "id",
        "student_id",
        "course_id",
        "term",
        "score",
        "enrolled_at",
        F.year(F.col("enrolled_at")).alias("enrolled_at_year"),
    )
)

enrollment_stats = (
    df_enrollments
    .groupBy("course_id")
    .agg(F.min("score").alias("min_enrollment_score"))
)

profiled = (
    cleaned_enrollments.alias("s")
    .join(enrollment_stats.alias("e"), F.col("e.course_id") == F.col("s.course_id"), "left")
    .select("s.*", "e.min_enrollment_score")
)

combined_details = (
    profiled.alias("s")
    .join(df_courses.alias("c"), (F.col("c.id") == F.col("s.course_id")) & (F.col("c.dept") != "Bio"), "inner")
    .join(df_students.alias("st"), F.col("st.id") == F.col("s.student_id"), "left")
    .select(
        "s.id",
        "s.student_id",
        "s.course_id",
        "s.score",
        "s.enrolled_at",
        F.col("c.level").alias("course_level"),
        F.col("st.gpa").alias("student_gpa"),
    )
)

totals = (
    combined_details
    .groupBy("course_id")
    .agg(
        F.round(F.sum("student_gpa"), 2).alias("total_student_gpa"),
        F.count("*").alias("cnt"),
        F.array_sort(F.collect_set("course_level")).alias("course_level_list"),
        F.percentile_approx("student_gpa", 0.5).alias("median_student_gpa"),
        F.sum(F.when(F.col("course_level").isNotNull(), 1).otherwise(0)).alias("n_course_level_is_not_null"),
    )
)

with_parent = (
    totals.alias("s")
    .join(df_courses.alias("c"), F.col("c.id") == F.col("s.course_id"), "left")
    .select("s.*", F.col("c.dept").alias("course_dept"))
)

result = (
    with_parent
    .select(
        "course_id",
        "total_student_gpa",
        "cnt",
        "course_level_list",
        "median_student_gpa",
        "n_course_level_is_not_null",
        "course_dept",
        F.size(F.col("course_level_list")).alias("course_level_list_count"),
        F.size(F.col("course_level_list")).alias("course_level_list_count_2"),
    )
    .orderBy(F.col("course_level_list_count_2").asc())
)
