from pyspark.sql import functions as F

prepared = (
    df_enrollments
    .filter(F.col("term").isNotNull())
    .select(
        "id",
        "student_id",
        "course_id",
        "score",
        "grade_status",
        "enrolled_at",
        F.coalesce(F.col("score"), F.lit(0.0)).alias("score_filled"),
    )
)

enriched_student = (
    prepared.alias("s")
    .join(df_students.alias("st"), F.col("st.id") == F.col("s.student_id"), "inner")
    .join(df_courses.alias("c"), F.col("c.id") == F.col("s.course_id"), "inner")
    .select(
        "s.id",
        "s.student_id",
        "s.course_id",
        "s.score",
        "s.enrolled_at",
        "s.score_filled",
        F.col("st.enrolled_on").alias("student_enrolled_on"),
        F.col("st.cohort").alias("student_cohort"),
        F.col("c.dept").alias("course_dept"),
        F.col("c.credits").alias("course_credits"),
    )
)

student_cohort_stats = (
    enriched_student
    .groupBy("student_cohort")
    .agg(F.percentile_approx("score_filled", 0.5).alias("median_score_filled"))
)

outliers = (
    enriched_student.alias("s")
    .join(student_cohort_stats.alias("g"), F.col("g.student_cohort") == F.col("s.student_cohort"), "inner")
    .filter(F.col("s.score_filled") >= F.col("g.median_score_filled"))
    .select(
        "s.*",
        "g.median_score_filled",
        F.round(F.col("s.score_filled") - F.col("g.median_score_filled"), 2).alias("score_filled_minus_median"),
    )
)

tagged = (
    outliers
    .select(
        "*",
        F.when(F.col("course_dept").isin("Bio", "Art"), "group_a").when(F.col("course_dept") == "Biz", "group_b").otherwise("other").alias("course_dept_group"),
        F.round(F.col("median_score_filled") * 1.5, 2).alias("median_score_filled_adj"),
    )
)

result = (
    tagged
    .filter(F.col("score_filled_minus_median") <= 20.0)
    .select(
        "id",
        "course_id",
        "enrolled_at",
        "score_filled",
        "course_dept",
        "course_credits",
        "median_score_filled",
        "score_filled_minus_median",
        "median_score_filled_adj",
    )
    .orderBy("score_filled", "median_score_filled", "id")
    .limit(10)
)
