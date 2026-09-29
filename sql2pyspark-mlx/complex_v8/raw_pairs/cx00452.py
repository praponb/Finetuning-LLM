from pyspark.sql import functions as F

filtered_enrollments = (
    df_enrollments
    .filter((~F.col("term").isin("2024T2", "2024T1")) | (F.col("grade_status") != "FAIL"))
    .select(
        "id",
        "student_id",
        "course_id",
        "term",
        "score",
        "grade_status",
        "enrolled_at",
        F.when(F.col("grade_status").isin("INCOMPLETE", "FAIL"), "group_a").when(F.col("grade_status") == "WITHDRAWN", "group_b").otherwise("other").alias("grade_status_group"),
        (F.col("enrolled_at") + F.expr("INTERVAL 7 DAYS")).alias("enrolled_at_plus_7d"),
    )
)

student_id_stats = (
    filtered_enrollments
    .groupBy("student_id")
    .agg(F.avg("score").alias("avg_score"))
)

compared = (
    filtered_enrollments.alias("s")
    .join(student_id_stats.alias("g"), F.col("g.student_id") == F.col("s.student_id"), "inner")
    .filter(F.col("s.score") > F.col("g.avg_score"))
    .select(
        "s.*",
        "g.avg_score",
        F.round(F.col("s.score") - F.col("g.avg_score"), 2).alias("score_minus_avg"),
    )
)

grade_status_group_stats = (
    compared
    .groupBy("grade_status_group")
    .agg(F.percentile_approx("score_minus_avg", 0.5).alias("median_score_minus_avg"))
)

above_average = (
    compared.alias("s")
    .join(grade_status_group_stats.alias("g"), F.col("g.grade_status_group") == F.col("s.grade_status_group"), "inner")
    .filter(F.col("s.score_minus_avg") >= F.col("g.median_score_minus_avg"))
    .select(
        "s.*",
        "g.median_score_minus_avg",
        F.round(F.col("s.score_minus_avg") - F.col("g.median_score_minus_avg"), 2).alias("score_minus_avg_minus_median"),
    )
)

result = (
    above_average
    .groupBy("enrolled_at_plus_7d", "term")
    .pivot("grade_status_group", ["group_b", "other", "group_a"])
    .agg(F.sum("score_minus_avg_minus_median"))
    .orderBy("enrolled_at_plus_7d", "term")
)
