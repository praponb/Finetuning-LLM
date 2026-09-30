from pyspark.sql import functions as F

recent_enrollments = (
    df_enrollments
    .filter((F.col("student_id").isNotNull()) & (F.col("term").isNotNull()))
    .select(
        "id",
        "student_id",
        "course_id",
        "term",
        "score",
        "grade_status",
        "enrolled_at",
        (F.floor(F.col("score") / 25) * 25).alias("score_bucket"),
    )
)

annotated = (
    recent_enrollments
    .filter(F.col("grade_status").isin("PASS", "INCOMPLETE", "WITHDRAWN"))
    .select(
        "*",
        F.initcap(F.trim(F.col("term"))).alias("term_clean"),
        F.date_format(F.col("enrolled_at"), "yyyy-MM").alias("enrolled_at_label"),
        F.round(F.col("score") / F.nullif(F.col("score_bucket"), F.lit(0)), 3).alias("score_per_score_bucket"),
    )
)

summary = (
    annotated
    .filter(F.col("score_per_score_bucket") < 7.0)
    .groupBy(
        "student_id",
        F.date_trunc("month", "enrolled_at").alias("enrolled_at_month"),
        "grade_status",
    )
    .agg(
        F.count("*").alias("n_rows"),
        F.sum("score_bucket").alias("total_score_bucket"),
        F.countDistinct("id").alias("distinct_ids"),
        F.min("enrolled_at").alias("first_enrolled_at"),
    )
)

enrollment_stats = (
    df_enrollments
    .groupBy("student_id")
    .agg(F.round(F.avg("score"), 2).alias("avg_enrollment_score"))
)

profiled = (
    summary.alias("s")
    .join(enrollment_stats.alias("e"), F.col("e.student_id") == F.col("s.student_id"), "left")
    .select("s.*", "e.avg_enrollment_score")
)

with_children_enrollments = (
    profiled.alias("s")
    .join(df_enrollments.alias("e"), F.col("e.student_id") == F.col("s.student_id"), "left")
    .select(
        "s.student_id",
        "s.enrolled_at_month",
        "s.grade_status",
        "s.n_rows",
        "s.total_score_bucket",
        "s.distinct_ids",
        "s.first_enrolled_at",
        F.col("e.id").alias("enrollment_id"),
        F.col("e.enrolled_at").alias("enrollment_enrolled_at"),
        F.col("e.course_id").alias("enrollment_course_id"),
    )
)

without_matches = (
    with_children_enrollments.alias("s")
    .join(df_courses.alias("c"), (F.col("c.id") == F.col("s.enrollment_course_id")) & (F.col("c.credits") < 4), "left_anti")
)

result = (
    without_matches
    .select(
        "student_id",
        "enrolled_at_month",
        "n_rows",
        "total_score_bucket",
        "distinct_ids",
        "first_enrolled_at",
        "enrollment_enrolled_at",
        "enrollment_course_id",
        F.when(F.col("total_score_bucket") >= 480, "A").when(F.col("total_score_bucket") >= 150, "B").otherwise("C").alias("total_score_bucket_tier"),
    )
    .orderBy(F.col("total_score_bucket_tier").asc())
)
