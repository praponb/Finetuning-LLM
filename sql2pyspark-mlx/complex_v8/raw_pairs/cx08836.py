from pyspark.sql import functions as F

matched = (
    df_enrollments.alias("a")
    .join(
        df_enrollments.alias("b"),
        (F.col("b.student_id") == F.col("a.student_id"))
        & (F.col("b.enrolled_at") < F.col("a.enrolled_at"))
        & (F.col("b.enrolled_at") >= F.col("a.enrolled_at") - F.expr("INTERVAL 1 DAYS")),
        "left",
    )
    .groupBy("a.id", "a.student_id", "a.enrolled_at")
    .agg(
        F.count("b.id").alias("prior_rows_1d"),
        F.sum("b.score").alias("prior_score_1d"),
    )
)

qualified = (
    matched.alias("s")
    .join(df_enrollments.alias("e"), (F.col("e.student_id") == F.col("s.student_id")) & (F.col("e.course_id").isNotNull()) & (F.col("e.grade_status") != "WITHDRAWN"), "left_semi")
)

totals = (
    qualified
    .groupBy(
        "prior_rows_1d",
        F.date_trunc("month", "enrolled_at").alias("enrolled_at_month"),
        "student_id",
    )
    .agg(
        F.max("enrolled_at").alias("last_enrolled_at"),
        F.min("prior_rows_1d").alias("min_prior_rows_1d"),
        F.min("enrolled_at").alias("first_enrolled_at"),
        F.count("*").alias("row_count"),
    )
)

result = (
    totals
    .filter(F.col("row_count") < 10)
    .select(
        "prior_rows_1d",
        "enrolled_at_month",
        "min_prior_rows_1d",
        "first_enrolled_at",
        "row_count",
        F.round(F.col("row_count") / F.nullif(F.col("prior_rows_1d"), F.lit(0)), 3).alias("row_count_per_prior_rows_1d"),
    )
    .orderBy(F.col("first_enrolled_at").asc())
)
