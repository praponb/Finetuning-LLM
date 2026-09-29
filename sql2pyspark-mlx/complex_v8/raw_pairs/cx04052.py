from pyspark.sql import functions as F

scoped_enrollments = (
    df_enrollments
    .filter((F.to_date(F.col("enrolled_at")) < F.lit("2025-03-08").cast("date")) & (F.col("course_id").isNotNull()))
    .select(
        "id",
        "student_id",
        "course_id",
        "term",
        "score",
        "grade_status",
        "enrolled_at",
        F.concat_ws("-", F.col("grade_status"), F.col("term")).alias("grade_status_term_label"),
        F.lower(F.regexp_replace(F.trim(F.col("grade_status")), " ", "_")).alias("grade_status_slug"),
        F.when(F.col("score") >= 57.8, "A").when(F.col("score") >= 32.7, "B").otherwise("C").alias("score_tier"),
    )
)

enrollment_stats = (
    df_enrollments
    .groupBy("course_id")
    .agg(
        F.max("enrolled_at").alias("last_enrollment_enrolled_at"),
        F.count("*").alias("enrollment_count"),
    )
)

summarized_rows = (
    scoped_enrollments.alias("s")
    .join(enrollment_stats.alias("e"), F.col("e.course_id") == F.col("s.course_id"), "left")
    .select(
        "s.*",
        "e.last_enrollment_enrolled_at",
        F.coalesce(F.col("e.enrollment_count"), F.lit(0)).alias("enrollment_count"),
    )
)

course_id_stats = (
    summarized_rows
    .groupBy("course_id")
    .agg(F.percentile_approx("score", 0.5).alias("median_score"))
)

compared = (
    summarized_rows.alias("s")
    .join(course_id_stats.alias("g"), F.col("g.course_id") == F.col("s.course_id"), "inner")
    .filter(F.col("s.score") >= F.col("g.median_score"))
    .select(
        "s.*",
        "g.median_score",
        F.round(F.col("s.score") - F.col("g.median_score"), 2).alias("score_minus_median"),
    )
)

with_children_enrollments = (
    compared.alias("s")
    .join(df_enrollments.alias("e"), (F.col("e.student_id") == F.col("s.student_id")) & (F.col("e.score") <= 83.1), "left")
    .select(
        "s.id",
        "s.student_id",
        "s.course_id",
        "s.score",
        "s.enrolled_at",
        "s.grade_status_slug",
        "s.last_enrollment_enrolled_at",
        "s.enrollment_count",
        F.col("e.id").alias("enrollment_id"),
        F.col("e.grade_status").alias("enrollment_grade_status"),
        F.col("e.course_id").alias("enrollment_course_id"),
        F.col("e.score").alias("enrollment_score"),
        F.col("e.term").alias("enrollment_term"),
    )
)

result = (
    with_children_enrollments
    .select(
        "id",
        "enrollment_id",
        "enrollment_score",
        "score",
    )
    .unpivot(
        ids=["id", "enrollment_id"],
        values=["enrollment_score", "score"],
        variableColumnName="field",
        valueColumnName="metric_value",
    )
    .filter(F.col("metric_value").isNotNull())
    .orderBy("id", "enrollment_id", "field")
)
