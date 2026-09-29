from pyspark.sql import functions as F

prepared_enrollments = (
    df_enrollments
    .filter((F.col("score").between(8.8, 78.8)) & (F.col("grade_status") == "FAIL"))
    .select(
        "id",
        "student_id",
        "course_id",
        "term",
        "score",
        "grade_status",
        "enrolled_at",
        F.round(F.col("score") * 1.07, 2).alias("score_adj"),
        F.length(F.trim(F.col("grade_status"))).alias("grade_status_len"),
        F.coalesce(F.col("term"), F.lit("UNKNOWN")).alias("term_or_unknown"),
    )
)

enriched = (
    prepared_enrollments.alias("s")
    .join(df_courses.alias("c"), F.col("c.id") == F.col("s.course_id"), "inner")
    .select(
        "s.id",
        "s.student_id",
        "s.course_id",
        "s.score",
        "s.grade_status",
        "s.grade_status_len",
        "s.term_or_unknown",
        F.col("c.level").alias("course_level"),
        F.col("c.dept").alias("course_dept"),
    )
)

rollup_base = (
    enriched
    .groupBy("course_dept")
    .agg(
        F.count("*").alias("n_rows"),
        F.sum(F.when(F.col("course_level").isin("intro", "core"), 1).otherwise(0)).alias("n_course_level_in_intro_core"),
        F.array_sort(F.collect_set("course_dept")).alias("course_dept_list"),
        F.count(F.when(F.col("course_level") == "advanced", 1)).alias("n_course_level_advanced"),
        F.countDistinct("course_level").alias("distinct_course_levels"),
        F.sum("grade_status_len").alias("total_grade_status_len"),
    )
    .filter(F.col("n_rows") >= 3)
)

scored = (
    rollup_base
    .filter(F.col("distinct_course_levels").between(1, 7))
    .select(
        "*",
        F.coalesce(F.col("course_dept"), F.lit("UNKNOWN")).alias("course_dept_or_unknown"),
        F.abs(F.col("n_rows") - 7).alias("n_rows_distance"),
        F.abs(F.col("n_rows") - 6).alias("n_rows_distance_2"),
    )
)

scored_2 = (
    scored
    .filter(F.col("course_dept_or_unknown").isin("Bio", "Math", "CS"))
    .select(
        "*",
        F.when(F.col("course_dept").isin("Art", "Math"), "group_a").when(F.col("course_dept") == "Bio", "group_b").otherwise("other").alias("course_dept_group"),
        F.coalesce(F.col("total_grade_status_len"), F.lit(0)).alias("total_grade_status_len_filled"),
        F.substring(F.trim(F.col("course_dept_or_unknown")), 1, 4).alias("course_dept_or_unknown_prefix"),
    )
)

without_matches = (
    scored_2
    .crossJoin(scored_2.agg(F.avg("total_grade_status_len_filled").alias("overall_avg_total_grade_status_len_filled")))
    .filter(F.col("total_grade_status_len_filled") > F.col("overall_avg_total_grade_status_len_filled"))
    .drop("overall_avg_total_grade_status_len_filled")
)

tagged = (
    without_matches
    .filter(F.col("course_dept_or_unknown_prefix").isNotNull())
    .select(
        "course_dept",
        "n_rows",
        "n_course_level_in_intro_core",
        "course_dept_list",
        "n_course_level_advanced",
        "distinct_course_levels",
        "total_grade_status_len",
        "course_dept_or_unknown",
        "n_rows_distance_2",
        "course_dept_group",
        "course_dept_or_unknown_prefix",
        F.substring(F.trim(F.col("course_dept")), 1, 2).alias("course_dept_prefix"),
        F.array_join(F.array_sort(F.col("course_dept_list")), ",").alias("course_dept_list_text"),
        F.substring(F.trim(F.col("course_dept_or_unknown")), 1, 4).alias("course_dept_or_unknown_prefix_2"),
    )
)

result = (
    tagged
    .filter(F.col("distinct_course_levels") >= 2)
    .select(
        "course_dept",
        "n_rows",
        "course_dept_list",
        "total_grade_status_len",
        "course_dept_group",
        "course_dept_or_unknown_prefix",
        "course_dept_list_text",
        F.concat_ws(" / ", F.col("course_dept_or_unknown"), F.col("course_dept_list_text")).alias("course_dept_or_unknown_course_dept_list_text_lab"),
        F.length(F.trim(F.col("course_dept_list_text"))).alias("course_dept_list_text_len"),
    )
)
