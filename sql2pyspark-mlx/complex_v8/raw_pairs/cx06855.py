from pyspark.sql import functions as F

covered = (
    df_enrollments.alias("a")
    .join(
        df_enrollments.alias("b"),
        (F.col("b.student_id") == F.col("a.student_id"))
        & (F.col("b.enrolled_at") < F.col("a.enrolled_at"))
        & (F.col("b.enrolled_at") >= F.col("a.enrolled_at") - F.expr("INTERVAL 12 HOURS")),
        "left",
    )
    .groupBy("a.id", "a.student_id", "a.enrolled_at")
    .agg(
        F.count("b.id").alias("prior_rows_12h"),
        F.sum("b.score").alias("prior_score_12h"),
    )
)

with_activity = (
    covered.alias("s")
    .join(df_enrollments.alias("e"), (F.col("e.student_id") == F.col("s.student_id")) & (F.col("e.course_id") <= 5), "left_semi")
    .filter(F.col("student_id").isNotNull())
)

result = (
    with_activity
    .select("id", "enrolled_at", "prior_score_12h")
    .orderBy(F.col("prior_score_12h").desc(), "id")
)
