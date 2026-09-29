from pyspark.sql import functions as F

base_enrollments = (
    df_enrollments
    .select(
        "id",
        "student_id",
        "course_id",
        "score",
        "grade_status",
        "enrolled_at",
        F.date_trunc("day", F.col("enrolled_at")).alias("enrolled_at_day"),
        F.when(F.col("term").isin("2024T1", "2025T2"), "group_a").when(F.col("term") == "2025T1", "group_b").otherwise("other").alias("term_group"),
        F.date_trunc("week", F.col("enrolled_at")).alias("enrolled_at_week"),
    )
)

qualified = (
    base_enrollments.alias("s")
    .join(df_enrollments.alias("e"), F.col("e.student_id") == F.col("s.student_id"), "left_semi")
    .filter(F.month(F.col("enrolled_at_day")).isin(2, 3))
)

per_group = (
    qualified
    .groupBy("term_group")
    .agg(
        F.sum(F.when(F.col("grade_status").isNotNull(), F.col("score")).otherwise(0)).alias("score_grade_status_is_not_null"),
        F.sum(F.when(F.col("grade_status").isin("INCOMPLETE", "PASS"), F.col("score"))).alias("score_grade_status_in_incomplete_p"),
        F.count("*").alias("cnt"),
        F.max("score").alias("max_score_check"),
    )
    .filter(F.col("max_score_check") > 13.9)
    .drop("max_score_check")
)

labeled = (
    per_group
    .select(
        "*",
        F.when(F.col("cnt") >= 10, "HIGH").when(F.col("cnt") >= 4, "MEDIUM").otherwise("LOW").alias("cnt_tier"),
    )
)

result = (
    labeled
    .groupBy("cnt_tier")
    .pivot("term_group", ["other", "group_b"])
    .agg(F.max("score_grade_status_is_not_null"))
)
