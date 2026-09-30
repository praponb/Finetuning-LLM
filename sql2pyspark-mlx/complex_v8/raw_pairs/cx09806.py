from pyspark.sql import functions as F

covered = (
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

enrollment_stats = (
    df_enrollments
    .groupBy("student_id")
    .agg(
        F.round(F.avg("score"), 2).alias("avg_enrollment_score"),
        F.min("enrolled_at").alias("first_enrollment_enrolled_at"),
    )
)

profiled = (
    covered.alias("s")
    .join(enrollment_stats.alias("e"), F.col("e.student_id") == F.col("s.student_id"), "left")
    .select("s.*", "e.avg_enrollment_score", "e.first_enrollment_enrolled_at")
)

per_group = (
    profiled
    .filter(F.col("prior_score_1d") <= 325.0)
    .groupBy("prior_rows_1d", "student_id")
    .agg(
        F.count("*").alias("cnt"),
        F.min("enrolled_at").alias("first_enrolled_at"),
    )
)

with_children_enrollments = (
    per_group.alias("s")
    .join(df_enrollments.alias("e"), (F.col("e.student_id") == F.col("s.student_id")) & (F.col("e.course_id").isNotNull()), "inner")
    .select(
        "s.prior_rows_1d",
        "s.student_id",
        "s.cnt",
        "s.first_enrolled_at",
        F.col("e.id").alias("enrollment_id"),
        F.col("e.grade_status").alias("enrollment_grade_status"),
        F.col("e.score").alias("enrollment_score"),
        F.col("e.course_id").alias("enrollment_course_id"),
    )
)

result = (
    with_children_enrollments
    .select("prior_rows_1d", "cnt", "enrollment_score")
    .orderBy(F.col("enrollment_score").desc(), "prior_rows_1d")
)
