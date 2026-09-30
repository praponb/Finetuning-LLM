from pyspark.sql import functions as F

prepared = (
    df_enrollments
    .filter((F.col("term") == "2024T1") & (F.col("student_id") <= 7))
    .select(
        "id",
        "student_id",
        "course_id",
        "term",
        "score",
        "grade_status",
        "enrolled_at",
        F.date_trunc("month", F.col("enrolled_at")).alias("enrolled_at_month"),
        F.when(F.col("score") >= 74.9, "A").when(F.col("score") >= 38.3, "B").otherwise("C").alias("score_tier"),
        F.datediff(F.lit("2025-06-30").cast("date"), F.col("enrolled_at")).alias("days_since_enrolled_at"),
    )
)

with_activity = (
    prepared.alias("s")
    .join(df_enrollments.alias("e"), (F.col("e.course_id") == F.col("s.course_id")) & (F.col("e.score") >= 39.4) & (F.col("e.student_id") >= 15), "left_semi")
    .filter(F.col("grade_status") == "PASS")
)

enriched_student = (
    with_activity.alias("s")
    .join(df_students.alias("st"), F.col("st.id") == F.col("s.student_id"), "left")
    .join(df_courses.alias("c"), F.col("c.id") == F.col("s.course_id"), "left")
    .select(
        "s.*",
        F.col("st.cohort").alias("student_cohort"),
        F.col("c.dept").alias("course_dept"),
    )
)

rollup_base = (
    enriched_student
    .groupBy(
        F.date_trunc("month", "enrolled_at_month").alias("enrolled_at_month_month"),
        "term",
    )
    .agg(
        F.round(F.sum("score") / F.nullif(F.sum("days_since_enrolled_at"), F.lit(0)), 3).alias("score_to_days_since_enrolled_at_ratio"),
        F.round(F.avg("days_since_enrolled_at"), 2).alias("avg_days_since_enrolled_at"),
        F.sum("days_since_enrolled_at").alias("total_days_since_enrolled_at"),
        F.avg("score").alias("avg_score_check"),
    )
    .filter(F.col("avg_score_check") > 38.1)
    .drop("avg_score_check")
)

qualified = (
    rollup_base
    .crossJoin(rollup_base.agg(F.avg("score_to_days_since_enrolled_at_ratio").alias("overall_avg_score_to_days_since_enrolled_at_rati")))
    .filter(F.col("score_to_days_since_enrolled_at_ratio") > F.col("overall_avg_score_to_days_since_enrolled_at_rati"))
    .drop("overall_avg_score_to_days_since_enrolled_at_rati")
)

result = (
    qualified
    .select(
        "enrolled_at_month_month",
        "term",
        "score_to_days_since_enrolled_at_ratio",
        "avg_days_since_enrolled_at",
    )
    .unpivot(
        ids=["enrolled_at_month_month", "term"],
        values=["score_to_days_since_enrolled_at_ratio", "avg_days_since_enrolled_at"],
        variableColumnName="field",
        valueColumnName="metric_value",
    )
    .filter(F.col("metric_value").isNotNull())
)
