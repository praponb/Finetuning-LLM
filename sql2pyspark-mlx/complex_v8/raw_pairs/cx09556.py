from pyspark.sql import functions as F

cleaned_enrollments = (
    df_enrollments
    .filter((F.col("score").between(20.2, 78.8)) & (F.col("student_id").isNotNull()))
    .select(
        "id",
        "student_id",
        "course_id",
        "term",
        "score",
        "grade_status",
        "enrolled_at",
        F.date_trunc("quarter", F.col("enrolled_at")).alias("enrolled_at_quarter"),
    )
)

grouped = (
    cleaned_enrollments
    .filter(F.col("student_id").isNotNull())
    .groupBy("course_id")
    .agg(
        F.count("*").alias("row_count"),
        F.count(F.when(F.col("term") == "2024T2", 1)).alias("n_term_2024t2"),
        F.round(F.sum("score"), 2).alias("total_score"),
        F.sum(F.when(F.col("grade_status") == "WITHDRAWN", F.col("score")).otherwise(0)).alias("score_grade_status_withdrawn"),
        F.sum(F.when(~F.col("term").isin("2025T2", "2024T1"), F.col("score")).otherwise(0)).alias("score_term_not_in_2025t2_2024t1"),
        F.array_sort(F.collect_set("grade_status")).alias("grade_status_list"),
        F.max("score").alias("max_score_check"),
    )
    .filter(F.col("max_score_check") > 10.8)
    .drop("max_score_check")
)

tagged = (
    grouped
    .filter(F.col("score_grade_status_withdrawn").between(150.0, 300.0))
    .select(
        "*",
        F.coalesce(F.col("n_term_2024t2"), F.lit(0)).alias("n_term_2024t2_filled"),
        F.array_join(F.array_sort(F.col("grade_status_list")), ",").alias("grade_status_list_text"),
        F.round(F.col("row_count") / F.nullif(F.col("total_score"), F.lit(0)), 3).alias("row_count_per_total_score"),
    )
)

with_parent = (
    tagged.alias("s")
    .join(df_courses.alias("c"), F.col("c.id") == F.col("s.course_id"), "inner")
    .select("s.*", "c.course_code", F.col("c.level").alias("course_level"))
)

with_parent_course = (
    with_parent.alias("s")
    .join(df_courses.alias("c"), F.col("c.id") == F.col("s.course_id"), "inner")
    .select(
        "s.course_id",
        "s.total_score",
        "s.score_grade_status_withdrawn",
        "s.score_term_not_in_2025t2_2024t1",
        "s.grade_status_list",
        "s.grade_status_list_text",
        "s.row_count_per_total_score",
        "s.course_level",
        F.col("c.level").alias("course_level_2"),
        F.col("c.credits").alias("course_credits"),
        F.col("c.dept").alias("course_dept"),
    )
)

enrollment_stats = (
    df_enrollments
    .groupBy("course_id")
    .agg(
        F.min("enrolled_at").alias("first_enrollment_enrolled_at"),
        F.min("enrolled_at").alias("first_enrollment_enrolled_at_2"),
    )
)

profiled = (
    with_parent_course.alias("s")
    .join(enrollment_stats.alias("e"), F.col("e.course_id") == F.col("s.course_id"), "left")
    .select(
        "s.*",
        "e.first_enrollment_enrolled_at",
        "e.first_enrollment_enrolled_at_2",
    )
)

result = (
    profiled
    .select(
        "course_id",
        "score_grade_status_withdrawn",
        "grade_status_list",
        "grade_status_list_text",
        "course_level",
        "course_level_2",
        "course_dept",
        "first_enrollment_enrolled_at_2",
    )
    .orderBy(F.col("course_level_2").asc_nulls_last(), F.col("course_level").asc())
)
