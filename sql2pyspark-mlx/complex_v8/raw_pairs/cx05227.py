from pyspark.sql import functions as F

recent = (
    df_enrollments
    .filter(F.col("course_id").isNotNull())
    .select(
        "id",
        "student_id",
        "course_id",
        "term",
        "score",
        "enrolled_at",
        F.length(F.trim(F.col("grade_status"))).alias("grade_status_len"),
        F.when(F.col("score") >= 78.8, "A").when(F.col("score") >= 36.6, "B").otherwise("C").alias("score_tier"),
    )
)

joined = (
    recent.alias("s")
    .join(df_courses.alias("c"), F.col("c.id") == F.col("s.course_id"), "inner")
    .join(df_students.alias("st"), F.col("st.id") == F.col("s.student_id"), "left")
    .select(
        "s.id",
        "s.student_id",
        "s.course_id",
        "s.term",
        "s.score",
        "s.enrolled_at",
        "s.score_tier",
        "c.course_code",
        F.col("c.credits").alias("course_credits"),
        F.col("st.enrolled_on").alias("student_enrolled_on"),
        F.col("st.gpa").alias("student_gpa"),
    )
)

course_id_stats = (
    joined
    .groupBy("course_id")
    .agg(F.avg("course_credits").alias("avg_course_credits"))
)

vs_group = (
    joined.alias("s")
    .join(course_id_stats.alias("g"), F.col("g.course_id") == F.col("s.course_id"), "inner")
    .filter(F.col("s.course_credits") > F.col("g.avg_course_credits"))
    .select(
        "s.*",
        "g.avg_course_credits",
        F.round(F.col("s.course_credits") - F.col("g.avg_course_credits"), 2).alias("course_credits_minus_avg"),
    )
)

qualified = (
    vs_group.alias("s")
    .join(df_enrollments.alias("e"), (F.col("e.student_id") == F.col("s.student_id")) & (F.hour(F.col("e.enrolled_at")).between(8, 12)) & (F.col("e.score") >= 34.2), "left_semi")
)

per_group = (
    qualified
    .groupBy(
        F.date_trunc("month", "student_enrolled_on").alias("student_enrolled_on_month"),
        "course_credits",
        "course_id",
    )
    .agg(
        F.round(F.sum("avg_course_credits") / F.nullif(F.sum("course_credits_minus_avg"), F.lit(0)), 3).alias("avg_course_credits_to_course_credits_minus_avg_r"),
        F.sum(F.when(F.upper(F.trim(F.col("course_code"))).like("C%"), F.col("course_credits_minus_avg"))).alias("course_credits_minus_avg_upper_trim_course_code_"),
        F.array_sort(F.collect_set("term")).alias("term_list"),
        F.max("course_credits").alias("max_course_credits_check"),
    )
    .filter(F.col("max_course_credits_check") > 2)
    .drop("max_course_credits_check")
)

qualified_2 = (
    per_group
    .crossJoin(per_group.agg(F.avg("course_credits_minus_avg_upper_trim_course_code_").alias("overall_avg_course_credits_minus_avg_upper_trim_")))
    .filter(F.col("course_credits_minus_avg_upper_trim_course_code_") > F.col("overall_avg_course_credits_minus_avg_upper_trim_"))
    .drop("overall_avg_course_credits_minus_avg_upper_trim_")
)

result = (
    qualified_2
    .select(
        "student_enrolled_on_month",
        "course_credits",
        F.col("avg_course_credits_to_course_credits_minus_avg_r").alias("m_avg_course_credits_to_course_credits_minus_avg"),
        F.col("course_credits_minus_avg_upper_trim_course_code_").alias("m_course_credits_minus_avg_upper_trim_course_cod"),
    )
    .unpivot(
        ids=["student_enrolled_on_month", "course_credits"],
        values=["m_avg_course_credits_to_course_credits_minus_avg", "m_course_credits_minus_avg_upper_trim_course_cod"],
        variableColumnName="metric",
        valueColumnName="value",
    )
    .filter(F.col("value").isNotNull())
    .orderBy("student_enrolled_on_month", "course_credits", "metric")
)
