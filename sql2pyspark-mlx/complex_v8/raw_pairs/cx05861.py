from pyspark.sql import functions as F

valid = (
    df_enrollments
    .filter(F.col("term").isNotNull())
    .select(
        "id",
        "student_id",
        "course_id",
        "term",
        "score",
        "enrolled_at",
        (F.col("enrolled_at") + F.expr("INTERVAL 30 MINUTES")).alias("enrolled_at_plus_30m"),
        F.concat_ws("|", F.col("grade_status"), F.col("term")).alias("grade_status_term_label"),
    )
)

labeled = (
    valid
    .filter(~F.col("term").isin("2025T1", "2024T2"))
    .select(
        "*",
        F.when(F.col("score") >= 58.8, "A").when(F.col("score") >= 35.8, "B").otherwise("C").alias("score_tier"),
        F.substring(F.trim(F.col("grade_status_term_label")), 1, 4).alias("grade_status_term_label_prefix"),
        F.round((F.unix_timestamp(F.col("enrolled_at_plus_30m")) - F.unix_timestamp(F.col("enrolled_at"))) / 3600, 1).alias("hours_enrolled_at_to_enrolled_at_plus_30m"),
    )
)

result = (
    labeled
    .select("id", "hours_enrolled_at_to_enrolled_at_plus_30m", "score")
    .unpivot(
        ids=["id"],
        values=["hours_enrolled_at_to_enrolled_at_plus_30m", "score"],
        variableColumnName="field",
        valueColumnName="metric_value",
    )
    .filter(F.col("metric_value").isNotNull())
    .orderBy("id", "field")
)
