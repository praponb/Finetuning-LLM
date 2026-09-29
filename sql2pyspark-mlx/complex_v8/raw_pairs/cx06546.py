from pyspark.sql import functions as F

scoped = (
    df_enrollments
    .filter((F.col("grade_status") == "PASS") & (F.col("student_id").isNotNull()))
    .select(
        "id",
        "student_id",
        "course_id",
        "term",
        "score",
        "enrolled_at",
        F.when(F.col("grade_status").isin("INCOMPLETE", "WITHDRAWN"), "group_a").when(F.col("grade_status") == "FAIL", "group_b").otherwise("other").alias("grade_status_group"),
        F.abs(F.col("score") - 57.3).alias("score_distance"),
    )
)

annotated = (
    scoped
    .filter(F.col("score_distance").between(20.0, 65.0))
    .select("*", F.substring(F.trim(F.col("term")), 1, 4).alias("term_prefix"))
)

filtered = (
    annotated.alias("s")
    .join(df_enrollments.alias("e"), (F.col("e.student_id") == F.col("s.student_id")) & (F.col("e.grade_status") == "FAIL") & (F.col("e.term").isin("2024T1", "2024T2", "2025T2")), "left_anti")
    .filter(F.col("term_prefix").isNotNull())
)

grade_status_group_stats = (
    filtered
    .groupBy("grade_status_group")
    .agg(F.avg("score").alias("avg_score"))
)

vs_group = (
    filtered.alias("s")
    .join(grade_status_group_stats.alias("g"), F.col("g.grade_status_group") == F.col("s.grade_status_group"), "inner")
    .filter(F.col("s.score") > F.col("g.avg_score"))
    .select(
        "s.*",
        "g.avg_score",
        F.round(F.col("s.score") - F.col("g.avg_score"), 2).alias("score_minus_avg"),
    )
)

result = (
    vs_group
    .groupBy("term", "enrolled_at")
    .pivot("grade_status_group", ["group_b", "other"])
    .agg(F.sum("avg_score"))
    .orderBy("term", "enrolled_at")
)
