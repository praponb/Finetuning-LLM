from pyspark.sql import functions as F

filtered_enrollments = (
    df_enrollments
    .filter((F.col("grade_status").isin("WITHDRAWN", "FAIL", "INCOMPLETE")) & (F.col("score") < 78.8))
    .select(
        "id",
        "student_id",
        "course_id",
        "term",
        "score",
        "enrolled_at",
        F.quarter(F.col("enrolled_at")).alias("enrolled_at_quarter"),
        F.date_trunc("year", F.col("enrolled_at")).alias("enrolled_at_year"),
        F.datediff(F.lit("2026-01-01").cast("date"), F.col("enrolled_at")).alias("days_since_enrolled_at"),
    )
)

enriched = (
    filtered_enrollments
    .select(
        "*",
        F.lower(F.regexp_replace(F.trim(F.col("term")), " ", "_")).alias("term_slug"),
        (F.col("enrolled_at_quarter") + F.col("score")).alias("enrolled_at_quarter_plus_score"),
        F.abs(F.col("days_since_enrolled_at") - 385).alias("days_since_enrolled_at_distance"),
    )
)

term_stats = (
    enriched
    .groupBy("term")
    .agg(F.avg("enrolled_at_quarter_plus_score").alias("avg_enrolled_at_quarter_plus_score"))
)

compared = (
    enriched.alias("s")
    .join(term_stats.alias("g"), F.col("g.term") == F.col("s.term"), "inner")
    .filter(F.col("s.enrolled_at_quarter_plus_score") > F.col("g.avg_enrolled_at_quarter_plus_score"))
    .select(
        "s.*",
        "g.avg_enrolled_at_quarter_plus_score",
        F.round(F.col("s.enrolled_at_quarter_plus_score") - F.col("g.avg_enrolled_at_quarter_plus_score"), 2).alias("enrolled_at_quarter_plus_score_minus_avg"),
    )
)

line_level = (
    compared.alias("s")
    .join(df_enrollments.alias("e"), F.col("e.student_id") == F.col("s.student_id"), "inner")
    .select(
        "s.id",
        "s.student_id",
        "s.course_id",
        "s.term",
        "s.enrolled_at",
        "s.enrolled_at_year",
        "s.term_slug",
        "s.days_since_enrolled_at_distance",
        "s.avg_enrolled_at_quarter_plus_score",
        F.col("e.id").alias("enrollment_id"),
        F.col("e.score").alias("enrollment_score"),
        F.col("e.course_id").alias("enrollment_course_id"),
        F.col("e.enrolled_at").alias("enrollment_enrolled_at"),
        F.col("e.grade_status").alias("enrollment_grade_status"),
    )
)

result = (
    line_level
    .groupingSets(
        [["term", "course_id", "enrollment_course_id"], ["term"]],
        "term", "course_id", "enrollment_course_id",
    )
    .agg(
        F.grouping_id().alias("grouping_level"),
        F.round(F.avg("enrollment_score"), 2).alias("avg_enrollment_score"),
        F.round(100 * F.count(F.when(~F.col("term").isin("2024T2", "2025T2"), 1)) / F.count("*"), 1).alias("pct_term_not_in_2024t2_2025t2"),
        F.min("enrolled_at").alias("first_enrolled_at"),
    )
    .select(
        F.coalesce(F.col("term"), F.lit("ALL")).alias("term"),
        "course_id",
        "enrollment_course_id",
        "grouping_level",
        "avg_enrollment_score",
        "pct_term_not_in_2024t2_2025t2",
        "first_enrolled_at",
    )
    .orderBy(
        F.col("term").asc_nulls_last(),
        F.col("course_id").asc_nulls_last(),
        F.col("enrollment_course_id").asc_nulls_last(),
    )
)
