from pyspark.sql import functions as F

scoped_enrollments = (
    df_enrollments
    .filter((F.col("course_id").isNotNull()) & (F.col("enrolled_at") < F.lit("2025-02-18 10:00:00").cast("timestamp")))
    .select(
        "id",
        "student_id",
        "course_id",
        "term",
        "score",
        "grade_status",
        "enrolled_at",
        F.length(F.trim(F.col("term"))).alias("term_len"),
        F.date_format(F.col("enrolled_at"), "yyyy-MM-dd").alias("enrolled_at_label"),
    )
)

enriched = (
    scoped_enrollments
    .filter(F.col("course_id") <= 7)
    .select(
        "*",
        (F.floor(F.col("term_len") / 2) * 2).alias("term_len_bucket"),
        F.upper(F.trim(F.col("grade_status"))).alias("grade_status_clean"),
        F.month(F.col("enrolled_at")).alias("enrolled_at_month"),
    )
)

qualified = (
    enriched.alias("s")
    .join(df_enrollments.alias("e"), (F.col("e.student_id") == F.col("s.student_id")) & (F.col("e.score") >= 11.3), "left_semi")
)

combined_details = (
    qualified.alias("s")
    .join(df_courses.alias("c"), F.col("c.id") == F.col("s.course_id"), "inner")
    .join(df_students.alias("st"), F.col("st.id") == F.col("s.student_id"), "inner")
    .select(
        "s.id",
        "s.student_id",
        "s.course_id",
        "s.term",
        "s.score",
        "s.grade_status",
        "s.enrolled_at",
        "s.term_len_bucket",
        "s.enrolled_at_month",
        F.col("c.credits").alias("course_credits"),
        F.col("c.dept").alias("course_dept"),
        "c.course_code",
        F.col("st.enrolled_on").alias("student_enrolled_on"),
    )
)

summary = (
    combined_details
    .groupBy(
        F.date_trunc("month", "student_enrolled_on").alias("student_enrolled_on_month"),
    )
    .agg(
        F.sum(F.when(F.col("course_dept").isin("Biz", "Math", "Art"), 1).otherwise(0)).alias("n_course_dept_in_biz_math_art"),
        F.round(F.avg("course_credits"), 2).alias("avg_course_credits"),
        F.min("score").alias("min_score"),
        F.sum("score").alias("total_score"),
        F.percentile_approx("term_len_bucket", 0.5).alias("median_term_len_bucket"),
        F.sum(F.when(~F.col("term").isin("2025T2", "2024T1"), F.col("score"))).alias("score_term_not_in_2025t2_2024t1"),
    )
)

tagged = summary.withColumn("min_score_bucket", F.floor(F.col("min_score") / 25) * 25)

labeled = (
    tagged
    .select(
        "student_enrolled_on_month",
        "avg_course_credits",
        "min_score",
        "total_score",
        "median_term_len_bucket",
        "min_score_bucket",
        F.round(F.col("min_score") * 1.1, 2).alias("min_score_adj"),
        (F.col("total_score") + F.col("avg_course_credits")).alias("total_score_plus_avg_course_credits"),
        F.greatest(F.col("median_term_len_bucket"), F.col("avg_course_credits")).alias("greatest_median_term_len_bucket_avg_course_credi"),
    )
)

result = (
    labeled
    .filter(F.col("min_score_bucket") > 15)
    .select(
        "student_enrolled_on_month",
        "avg_course_credits",
        "total_score",
        "median_term_len_bucket",
        "min_score_adj",
        "total_score_plus_avg_course_credits",
        "greatest_median_term_len_bucket_avg_course_credi",
        F.abs(F.col("min_score_adj") - 45.0).alias("min_score_adj_distance"),
    )
    .orderBy(F.col("median_term_len_bucket").asc())
)
