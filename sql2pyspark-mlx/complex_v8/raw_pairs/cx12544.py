from pyspark.sql import functions as F

base = (
    df_enrollments
    .filter((F.col("score") > 32.7) | (F.col("student_id") <= 5))
    .select(
        "id",
        "student_id",
        "course_id",
        "term",
        "score",
        "enrolled_at",
        (F.col("enrolled_at") + F.expr("INTERVAL 2 HOURS")).alias("enrolled_at_plus_2h"),
        F.coalesce(F.col("term"), F.lit("UNKNOWN")).alias("term_or_unknown"),
        F.abs(F.col("score") - 62.5).alias("score_distance"),
    )
)

qualified = (
    base.alias("s")
    .join(df_enrollments.alias("e"), F.col("e.course_id") == F.col("s.course_id"), "left_semi")
)

result = (
    qualified
    .groupingSets(
        [["term_or_unknown", "course_id"], ["term_or_unknown"]],
        "term_or_unknown", "course_id",
    )
    .agg(F.max("enrolled_at").alias("last_enrolled_at"))
    .select(
        F.coalesce(F.col("term_or_unknown"), F.lit("ALL")).alias("term_or_unknown"),
        "course_id",
        "last_enrolled_at",
    )
)
