from pyspark.sql import functions as F

filtered_enrollments = (
    df_enrollments
    .filter((F.col("grade_status").isin("FAIL", "PASS")) & (F.col("term") == "2024T2"))
    .select(
        "id",
        "student_id",
        "course_id",
        "term",
        "score",
        "grade_status",
        "enrolled_at",
        F.coalesce(F.col("grade_status"), F.lit("UNKNOWN")).alias("grade_status_or_unknown"),
        F.to_date(F.col("enrolled_at")).alias("enrolled_at_date"),
        F.when(F.col("term").isin("2024T1", "2024T2"), "group_a").when(F.col("term") == "2025T2", "group_b").otherwise("other").alias("term_group"),
    )
)

joined_student = (
    filtered_enrollments.alias("s")
    .join(df_students.alias("st"), F.col("st.id") == F.col("s.student_id"), "left")
    .join(df_courses.alias("c"), F.col("c.id") == F.col("s.course_id"), "inner")
    .select(
        "s.*",
        "st.student_name",
        F.col("c.credits").alias("course_credits"),
        F.col("c.level").alias("course_level"),
    )
)

derived = (
    joined_student
    .select(
        "*",
        (F.col("enrolled_at") + F.expr("INTERVAL 1 DAYS")).alias("enrolled_at_plus_1d"),
        (F.col("score") - F.col("course_credits")).alias("score_minus_course_credits"),
    )
)

screened = (
    derived.alias("s")
    .join(df_enrollments.alias("e"), (F.col("e.course_id") == F.col("s.course_id")) & (F.col("e.grade_status") != "WITHDRAWN"), "left_semi")
)

result = (
    screened
    .groupingSets(
        [["term_group", "grade_status_or_unknown", "grade_status"], ["term_group"], ["grade_status_or_unknown"], []],
        "term_group", "grade_status_or_unknown", "grade_status",
    )
    .agg(
        F.grouping_id().alias("grouping_level"),
        F.sum(F.when(F.col("student_name").isNotNull(), F.col("score_minus_course_credits")).otherwise(0)).alias("score_minus_course_credits_student_name_is_not_n"),
        F.array_sort(F.collect_set("term")).alias("term_list"),
    )
    .select(
        F.coalesce(F.col("term_group"), F.lit("ALL")).alias("term_group"),
        F.coalesce(F.col("grade_status_or_unknown"), F.lit("ALL")).alias("grade_status_or_unknown"),
        F.coalesce(F.col("grade_status"), F.lit("ALL")).alias("grade_status"),
        "grouping_level",
        "score_minus_course_credits_student_name_is_not_n",
        "term_list",
    )
)
