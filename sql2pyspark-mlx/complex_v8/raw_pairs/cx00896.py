from pyspark.sql import functions as F

matched = (
    df_enrollments.alias("a")
    .join(
        df_enrollments.alias("b"),
        (F.col("b.student_id") == F.col("a.student_id"))
        & (F.col("b.enrolled_at") < F.col("a.enrolled_at"))
        & (F.col("b.enrolled_at") >= F.col("a.enrolled_at") - F.expr("INTERVAL 3 DAYS")),
        "left",
    )
    .groupBy("a.id", "a.student_id", "a.enrolled_at")
    .agg(
        F.count("b.id").alias("prior_rows_3d"),
        F.sum("b.score").alias("prior_score_3d"),
    )
)

with_activity = (
    matched.alias("s")
    .join(df_enrollments.alias("e"), (F.col("e.student_id") == F.col("s.student_id")) & (F.col("e.score").between(4.9, 85.8)), "left_anti")
)

annotated = (
    with_activity
    .select(
        "id",
        "student_id",
        "enrolled_at",
        "prior_score_3d",
        F.date_trunc("day", F.col("enrolled_at")).alias("enrolled_at_day"),
    )
)

result = (
    annotated
    .filter(F.col("student_id").isNotNull())
    .select(
        "id",
        "prior_score_3d",
        "enrolled_at_day",
        F.year(F.col("enrolled_at_day")).alias("enrolled_at_day_year"),
    )
    .orderBy(
        F.col("enrolled_at_day_year").asc_nulls_last(),
        F.col("prior_score_3d").desc(),
    )
)
