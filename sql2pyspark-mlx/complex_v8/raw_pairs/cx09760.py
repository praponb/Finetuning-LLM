from pyspark.sql import functions as F

filtered = (
    df_enrollments
    .filter((F.col("student_id") <= 13) & (F.to_date(F.col("enrolled_at")) < F.lit("2025-03-08").cast("date")))
    .select(
        "id",
        "student_id",
        "course_id",
        "term",
        "score",
        "grade_status",
    )
)

grade_status_stats = (
    filtered
    .groupBy("grade_status")
    .agg(F.max("score").alias("max_score"))
)

vs_group = (
    filtered.alias("s")
    .join(grade_status_stats.alias("g"), F.col("g.grade_status") == F.col("s.grade_status"), "inner")
    .filter(F.col("s.score") == F.col("g.max_score"))
    .select(
        "s.*",
        "g.max_score",
        F.round(F.col("s.score") - F.col("g.max_score"), 2).alias("score_minus_max"),
    )
)

with_parent = (
    vs_group.alias("s")
    .join(df_courses.alias("c"), F.col("c.id") == F.col("s.course_id"), "left")
    .join(df_students.alias("st"), F.col("st.id") == F.col("s.student_id"), "left")
    .select(
        "s.id",
        "s.student_id",
        "s.course_id",
        "s.term",
        "s.score",
        "s.grade_status",
        "s.max_score",
        "s.score_minus_max",
        F.col("c.level").alias("course_level"),
        F.col("c.credits").alias("course_credits"),
        F.col("c.dept").alias("course_dept"),
        "st.student_name",
        F.col("st.cohort").alias("student_cohort"),
    )
)

rollup_base = (
    with_parent
    .groupBy("course_dept", "student_id", "student_cohort")
    .agg(
        F.count("*").alias("cnt"),
        F.countDistinct("student_cohort").alias("distinct_student_cohorts"),
        F.countDistinct("term").alias("distinct_terms"),
        F.array_sort(F.collect_set("course_level")).alias("course_level_list"),
        F.sum(F.when(F.col("student_cohort").isin("C2022", "C2023"), 1).otherwise(0)).alias("n_student_cohort_in_c2022_c202"),
    )
)

joined_student = (
    rollup_base.alias("s")
    .join(df_students.alias("st"), F.col("st.id") == F.col("s.student_id"), "inner")
    .filter(F.col("st.major") == "Math")
    .select(
        "s.course_dept",
        "s.student_id",
        "s.student_cohort",
        "s.cnt",
        "s.distinct_student_cohorts",
        "s.distinct_terms",
        "s.course_level_list",
        F.col("st.is_international").alias("student_is_international"),
        F.col("st.cohort").alias("student_cohort_2"),
        F.col("st.major").alias("student_major"),
    )
)

result = (
    joined_student
    .groupingSets(
        [["student_id", "distinct_terms", "student_is_international"], ["student_id"], []],
        "student_id", "distinct_terms", "student_is_international",
    )
    .agg(
        F.array_sort(F.collect_set("student_cohort")).alias("student_cohort_list"),
    )
    .orderBy(
        F.col("student_id").asc_nulls_last(),
        F.col("distinct_terms").asc_nulls_last(),
        F.col("student_is_international").asc_nulls_last(),
    )
)
