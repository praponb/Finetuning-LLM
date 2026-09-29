from pyspark.sql import functions as F

joined_ranges = (
    df_enrollments.alias("a")
    .join(
        df_enrollments.alias("b"),
        (F.col("b.student_id") == F.col("a.student_id"))
        & (F.col("b.enrolled_at") < F.col("a.enrolled_at"))
        & (F.col("b.enrolled_at") >= F.col("a.enrolled_at") - F.expr("INTERVAL 3 DAYS")),
        "left",
    )
    .groupBy("a.id", "a.student_id", "a.enrolled_at")
    .agg(
        F.count("b.id").alias("prior_rows_3d"),
        F.sum("b.score").alias("prior_score_3d"),
    )
)

enriched = (
    joined_ranges.alias("s")
    .join(df_students.alias("st"), F.col("st.id") == F.col("s.student_id"), "inner")
    .filter(F.col("st.cohort") == "C2023")
    .select(
        "s.id",
        "s.student_id",
        "s.enrolled_at",
        "s.prior_rows_3d",
        F.col("st.cohort").alias("student_cohort"),
    )
)

summary = (
    enriched
    .groupBy(
        "prior_rows_3d",
        F.date_trunc("month", "enrolled_at").alias("enrolled_at_month"),
    )
    .agg(
        F.array_sort(F.collect_set("student_cohort")).alias("student_cohort_list"),
        F.countDistinct("student_id").alias("distinct_student_ids"),
        F.round(F.stddev("prior_rows_3d"), 2).alias("stddev_prior_rows_3d"),
    )
)

filtered = (
    summary
    .crossJoin(summary.agg(F.avg("distinct_student_ids").alias("overall_avg_distinct_student_ids")))
    .filter(F.col("distinct_student_ids") > F.col("overall_avg_distinct_student_ids"))
    .drop("overall_avg_distinct_student_ids")
)

result = (
    filtered
    .select(
        "prior_rows_3d",
        "enrolled_at_month",
        "student_cohort_list",
        "distinct_student_ids",
        "stddev_prior_rows_3d",
    )
    .orderBy(F.col("enrolled_at_month").desc(), "prior_rows_3d")
    .limit(20)
)
