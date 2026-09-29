from pyspark.sql import functions as F

recent = (
    df_enrollments
    .filter((F.col("student_id").isNotNull()) & (F.col("term") != "2025T1"))
    .select(
        "id",
        "student_id",
        "course_id",
        "term",
        "score",
        "enrolled_at",
    )
)

scored = (
    recent
    .filter(F.col("student_id") <= 9)
    .select(
        "*",
        F.coalesce(F.col("score"), F.lit(0.0)).alias("score_filled"),
        F.year(F.col("enrolled_at")).alias("enrolled_at_year"),
        F.date_trunc("quarter", F.col("enrolled_at")).alias("enrolled_at_quarter"),
    )
)

derived = (
    scored
    .filter(F.col("course_id").isNotNull())
    .select(
        "id",
        "student_id",
        "course_id",
        "term",
        "score",
        "enrolled_at",
        "enrolled_at_year",
        "enrolled_at_quarter",
        (F.col("enrolled_at") + F.expr("INTERVAL 1 DAYS")).alias("enrolled_at_plus_1d"),
        F.substring(F.trim(F.col("term")), 1, 4).alias("term_prefix"),
        F.round(F.col("score_filled") / F.nullif(F.col("enrolled_at_year"), F.lit(0)), 3).alias("score_filled_per_enrolled_at_year"),
    )
)

screened = (
    derived.alias("s")
    .join(df_enrollments.alias("e"), (F.col("e.student_id") == F.col("s.student_id")) & (F.col("e.grade_status") == "PASS"), "left_anti")
)

result = (
    screened
    .filter(F.col("term_prefix").isNotNull())
    .select(
        "id",
        "student_id",
        "course_id",
        "term",
        "score",
        "enrolled_at",
        "enrolled_at_year",
        "enrolled_at_quarter",
        "enrolled_at_plus_1d",
        "term_prefix",
        F.round((F.unix_timestamp(F.col("enrolled_at")) - F.unix_timestamp(F.col("enrolled_at_quarter"))) / 60, 1).alias("minutes_enrolled_at_quarter_to_enrolled_at"),
    )
)
