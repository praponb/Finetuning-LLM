from pyspark.sql import functions as F

recent = (
    df_enrollments
    .filter(F.col("score") >= 34.2)
    .select(
        "id",
        "student_id",
        "course_id",
        "term",
        "score",
        "grade_status",
        "enrolled_at",
        F.date_format(F.col("enrolled_at"), "yyyy-MM-dd").alias("enrolled_at_label"),
        (F.floor(F.col("score") / 25) * 25).alias("score_bucket"),
    )
)

with_activity = (
    recent
    .crossJoin(recent.agg(F.max("score_bucket").alias("overall_max_score_bucket")))
    .filter(F.col("score_bucket") < F.col("overall_max_score_bucket"))
    .drop("overall_max_score_bucket")
)

with_activity_2 = (
    with_activity
    .crossJoin(with_activity.agg(F.max("score_bucket").alias("overall_max_score_bucket")))
    .filter(F.col("score_bucket") < F.col("overall_max_score_bucket"))
    .drop("overall_max_score_bucket")
)

result = (
    with_activity_2
    .rollup("student_id", "grade_status")
    .agg(
        F.grouping("student_id").alias("is_total_student_id"),
        F.count("*").alias("cnt"),
        F.countDistinct("id").alias("distinct_ids"),
        F.sum(F.when(F.col("grade_status") == "PASS", F.col("score_bucket"))).alias("score_bucket_grade_status_pass"),
    )
    .select(
        "student_id",
        F.coalesce(F.col("grade_status"), F.lit("ALL")).alias("grade_status"),
        "is_total_student_id",
        "cnt",
        "distinct_ids",
        "score_bucket_grade_status_pass",
    )
    .orderBy(
        F.col("student_id").asc_nulls_last(),
        F.col("grade_status").asc_nulls_last(),
    )
)
