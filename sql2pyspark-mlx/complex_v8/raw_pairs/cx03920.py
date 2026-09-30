from pyspark.sql import functions as F

filtered_enrollments = (
    df_enrollments
    .filter(F.col("enrolled_at") < F.lit("2025-03-08 09:00:00").cast("timestamp"))
    .select(
        "id",
        "student_id",
        "course_id",
        "term",
        "score",
        "enrolled_at",
    )
)

totals = (
    filtered_enrollments
    .groupBy("course_id", F.to_date("enrolled_at").alias("enrolled_at_day"))
    .agg(
        F.max("enrolled_at").alias("last_enrolled_at"),
        F.max("score").alias("max_score"),
        F.avg("score").alias("avg_score_check"),
    )
    .filter(F.col("avg_score_check") > 30.0)
    .drop("avg_score_check")
)

filtered = (
    totals.alias("s")
    .join(df_enrollments.alias("e"), (F.col("e.course_id") == F.col("s.course_id")) & (F.col("e.score").between(8.7, 87.8)) & (F.col("e.grade_status").isin("PASS", "INCOMPLETE")), "left_anti")
)

enrollment_stats = (
    df_enrollments
    .groupBy("course_id")
    .agg(
        F.count(F.when(F.col("term").isin("2025T2", "2024T2", "2025T1"), 1)).alias("enrollment_term_count"),
        F.max("enrolled_at").alias("last_enrollment_enrolled_at"),
        F.min("score").alias("min_enrollment_score"),
    )
)

summarized_rows = (
    filtered.alias("s")
    .join(enrollment_stats.alias("e"), F.col("e.course_id") == F.col("s.course_id"), "left")
    .select(
        "s.*",
        F.coalesce(F.col("e.enrollment_term_count"), F.lit(0)).alias("enrollment_term_count"),
        "e.last_enrollment_enrolled_at",
        "e.min_enrollment_score",
    )
)

result = (
    summarized_rows
    .select(
        "course_id",
        "enrolled_at_day",
        "min_enrollment_score",
        "max_score",
    )
    .unpivot(
        ids=["course_id", "enrolled_at_day"],
        values=["min_enrollment_score", "max_score"],
        variableColumnName="field",
        valueColumnName="value",
    )
)
