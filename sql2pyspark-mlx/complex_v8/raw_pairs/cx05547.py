from pyspark.sql import functions as F

base = (
    df_enrollments
    .filter((F.col("grade_status") == "INCOMPLETE") & (F.col("course_id").isNotNull()))
    .select(
        "id",
        "student_id",
        "course_id",
        "term",
        "score",
        "grade_status",
        "enrolled_at",
        F.date_format(F.col("enrolled_at"), "yyyy-MM-dd HH:00").alias("enrolled_at_label"),
        F.coalesce(F.col("score"), F.lit(0.0)).alias("score_filled"),
    )
)

enriched = (
    base
    .filter(~F.col("term").isin("2025T2", "2024T1"))
    .select(
        "id",
        "student_id",
        "course_id",
        "grade_status",
        "enrolled_at_label",
        "score_filled",
        F.coalesce(F.col("score_filled"), F.lit(0.0)).alias("score_filled_filled"),
        F.date_format(F.col("enrolled_at"), "MMM yyyy").alias("enrolled_at_label_2"),
    )
)

result = (
    enriched
    .groupingSets(
        [["course_id", "grade_status"], ["course_id"], []],
        "course_id", "grade_status",
    )
    .agg(
        F.grouping_id().alias("grouping_level"),
        F.round(F.stddev("score_filled"), 2).alias("stddev_score_filled"),
        F.array_sort(F.collect_set("grade_status")).alias("grade_status_list"),
        F.countDistinct("course_id").alias("distinct_course_ids"),
    )
    .orderBy(
        F.col("course_id").asc_nulls_last(),
        F.col("grade_status").asc_nulls_last(),
    )
)
