from pyspark.sql import functions as F

prepared = (
    df_enrollments
    .filter(~F.col("term").isin("2025T1", "2025T2"))
    .select(
        "id",
        "student_id",
        "course_id",
        "term",
        "score",
        "grade_status",
        "enrolled_at",
        F.coalesce(F.col("score"), F.lit(0.0)).alias("score_filled"),
    )
)

without_matches = (
    prepared.alias("s")
    .join(df_enrollments.alias("e"), (F.col("e.student_id") == F.col("s.student_id")) & (F.col("e.score") > 38.1) & (F.col("e.course_id").isNotNull()), "left_anti")
    .filter(~F.col("grade_status").isin("PASS", "INCOMPLETE"))
)

enriched = (
    without_matches
    .filter(F.col("student_id") >= 14)
    .select(
        "*",
        F.coalesce(F.col("grade_status"), F.lit("UNKNOWN")).alias("grade_status_or_unknown"),
        F.substring(F.trim(F.col("term")), 1, 2).alias("term_prefix"),
    )
)

rollup_base = (
    enriched
    .filter(F.col("score") < 83.1)
    .groupBy("course_id")
    .agg(
        F.min("enrolled_at").alias("first_enrolled_at"),
        F.count("*").alias("cnt"),
    )
    .filter(F.col("cnt") >= 2)
)

result = (
    rollup_base
    .select(
        "course_id",
        "first_enrolled_at",
        F.quarter(F.col("first_enrolled_at")).alias("first_enrolled_at_quarter"),
        F.date_trunc("quarter", F.col("first_enrolled_at")).alias("first_enrolled_at_quarter_2"),
    )
    .orderBy("first_enrolled_at_quarter", F.col("first_enrolled_at").desc())
)
