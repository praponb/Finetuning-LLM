from pyspark.sql import functions as F

prepared_enrollments = (
    df_enrollments
    .filter((F.col("grade_status") != "INCOMPLETE") & (F.col("term") != "2025T1"))
    .select(
        "id",
        "student_id",
        "course_id",
        "term",
        "score",
        "grade_status",
        "enrolled_at",
        F.coalesce(F.col("score"), F.lit(0.0)).alias("score_filled"),
        F.coalesce(F.col("score"), F.lit(0.0)).alias("score_filled_2"),
        F.date_format(F.col("enrolled_at"), "MMM yyyy").alias("enrolled_at_label"),
    )
)

enriched = (
    prepared_enrollments
    .filter(F.col("score_filled") >= 20.0)
    .withColumn("term_group", F.when(F.col("term").isin("2024T1", "2025T1"), "group_a").when(F.col("term") == "2025T2", "group_b").otherwise("other"))
)

qualified = (
    enriched.alias("s")
    .join(df_enrollments.alias("e"), (F.col("e.student_id") == F.col("s.student_id")) & (F.col("e.term") == "2025T1"), "left_anti")
)

result = (
    qualified
    .rollup("grade_status", "term")
    .agg(
        F.grouping_id().alias("gid"),
        F.max("enrolled_at").alias("last_enrolled_at"),
    )
)
