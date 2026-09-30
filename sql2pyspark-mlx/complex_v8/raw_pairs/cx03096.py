from pyspark.sql import functions as F

joined_ranges = (
    df_enrollments.alias("a")
    .join(
        df_enrollments.alias("b"),
        (F.col("b.student_id") == F.col("a.student_id"))
        & (F.col("b.enrolled_at") < F.col("a.enrolled_at"))
        & (F.col("b.enrolled_at") >= F.col("a.enrolled_at") - F.expr("INTERVAL 7 DAYS")),
        "left",
    )
    .groupBy("a.id", "a.student_id", "a.enrolled_at")
    .agg(
        F.count("b.id").alias("prior_rows_7d"),
        F.sum("b.score").alias("prior_score_7d"),
    )
)

per_group = (
    joined_ranges
    .groupBy(
        F.date_trunc("month", "enrolled_at").alias("enrolled_at_month"),
        "prior_rows_7d",
        "student_id",
    )
    .agg(
        F.count("*").alias("cnt"),
        F.min("prior_score_7d").alias("min_prior_score_7d"),
        F.max("enrolled_at").alias("last_enrolled_at"),
    )
    .filter(F.col("cnt") >= 2)
)

combined_details = (
    per_group.alias("s")
    .join(df_students.alias("st"), (F.col("st.id") == F.col("s.student_id")) & (F.upper(F.trim(F.col("st.student_name"))).like("H%")), "left")
    .select("s.*", F.col("st.cohort").alias("student_cohort"))
)

with_children_enrollments = (
    combined_details.alias("s")
    .join(df_enrollments.alias("e"), F.col("e.student_id") == F.col("s.student_id"), "left")
    .select(
        "s.enrolled_at_month",
        "s.prior_rows_7d",
        "s.student_id",
        "s.cnt",
        "s.min_prior_score_7d",
        "s.last_enrolled_at",
        "s.student_cohort",
        F.col("e.id").alias("enrollment_id"),
        F.col("e.term").alias("enrollment_term"),
        F.col("e.course_id").alias("enrollment_course_id"),
        F.col("e.grade_status").alias("enrollment_grade_status"),
        F.col("e.enrolled_at").alias("enrollment_enrolled_at"),
    )
)

result = (
    with_children_enrollments
    .select(
        "enrolled_at_month",
        "prior_rows_7d",
        "student_id",
        "cnt",
        "last_enrolled_at",
        "enrollment_term",
        "enrollment_course_id",
        "enrollment_enrolled_at",
        F.round(F.col("min_prior_score_7d") / F.nullif(F.col("prior_rows_7d"), F.lit(0)), 3).alias("min_prior_score_7d_per_prior_rows_7d"),
        F.to_date(F.col("enrollment_enrolled_at")).alias("enrollment_enrolled_at_date"),
    )
    .orderBy("enrollment_term", F.col("enrolled_at_month").desc())
)
