from pyspark.sql import functions as F

recent = (
    df_enrollments
    .filter(F.col("term") == "2025T2")
    .select(
        "id",
        "student_id",
        "course_id",
        "term",
        "score",
        "grade_status",
        "enrolled_at",
        F.length(F.trim(F.col("grade_status"))).alias("grade_status_len"),
        F.date_format(F.col("enrolled_at"), "yyyy-MM").alias("enrolled_at_label"),
    )
)

without_matches = (
    recent
    .crossJoin(recent.agg(F.max("score").alias("overall_max_score")))
    .filter(F.col("score") < F.col("overall_max_score"))
    .drop("overall_max_score")
)

enriched = without_matches.withColumn("term_prefix", F.substring(F.trim(F.col("term")), 1, 2))

result = (
    enriched
    .groupBy("student_id")
    .pivot("grade_status", ["WITHDRAWN", "FAIL", "INCOMPLETE"])
    .agg(F.max("score"))
    .orderBy("student_id")
)
