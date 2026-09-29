from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_grade_status_by_score_minus = Window.partitionBy("grade_status").orderBy("score_minus_avg", "id")

filtered_enrollments = (
    df_enrollments
    .filter(F.col("score") >= 16.3)
    .select(
        "id",
        "student_id",
        "course_id",
        "term",
        "score",
        "grade_status",
        "enrolled_at",
    )
)

filtered = (
    filtered_enrollments
    .crossJoin(filtered_enrollments.agg(F.max("score").alias("overall_max_score")))
    .filter(F.col("score") < F.col("overall_max_score"))
    .drop("overall_max_score")
)

term_stats = (
    filtered
    .groupBy("term")
    .agg(F.avg("score").alias("avg_score"))
)

above_average = (
    filtered.alias("s")
    .join(term_stats.alias("g"), F.col("g.term") == F.col("s.term"), "inner")
    .filter(F.col("s.score") > F.col("g.avg_score"))
    .select(
        "s.*",
        "g.avg_score",
        F.round(F.col("s.score") - F.col("g.avg_score"), 2).alias("score_minus_avg"),
    )
)

term_stats_2 = (
    above_average
    .groupBy("term")
    .agg(F.avg("score_minus_avg").alias("avg_score_minus_avg"))
)

vs_group = (
    above_average.alias("s")
    .join(term_stats_2.alias("g"), F.col("g.term") == F.col("s.term"), "inner")
    .filter(F.col("s.score_minus_avg") > F.col("g.avg_score_minus_avg"))
    .select(
        "s.*",
        "g.avg_score_minus_avg",
        F.round(F.col("s.score_minus_avg") - F.col("g.avg_score_minus_avg"), 2).alias("score_minus_avg_minus_avg"),
    )
)

sequenced = (
    vs_group
    .select(
        "id",
        "student_id",
        "course_id",
        "term",
        "score",
        "enrolled_at",
        "avg_score",
        "score_minus_avg",
        "avg_score_minus_avg",
        "score_minus_avg_minus_avg",
        F.rank().over(w_grade_status_by_score_minus).alias("score_minus_avg_rank"),
        F.round(F.cume_dist().over(w_grade_status_by_score_minus), 3).alias("score_minus_avg_cume_dist"),
        F.row_number().over(w_grade_status_by_score_minus).alias("rn"),
    )
)

enrollment_stats = (
    df_enrollments
    .groupBy("student_id")
    .agg(
        F.min("enrolled_at").alias("first_enrollment_enrolled_at"),
        F.min("enrolled_at").alias("first_enrollment_enrolled_at_2"),
    )
)

summarized_rows = (
    sequenced.alias("s")
    .join(enrollment_stats.alias("e"), F.col("e.student_id") == F.col("s.student_id"), "left")
    .select(
        "s.*",
        "e.first_enrollment_enrolled_at",
        "e.first_enrollment_enrolled_at_2",
    )
)

annotated = (
    summarized_rows
    .filter(F.col("rn") <= 8)
    .select(
        "id",
        "student_id",
        "course_id",
        "term",
        "score",
        "enrolled_at",
        "avg_score",
        "avg_score_minus_avg",
        "score_minus_avg_minus_avg",
        "score_minus_avg_cume_dist",
        "rn",
        "first_enrollment_enrolled_at",
        F.date_trunc("quarter", F.col("enrolled_at")).alias("enrolled_at_quarter"),
        F.when(F.col("term").isin("2025T1", "2025T2"), "group_a").when(F.col("term") == "2024T1", "group_b").otherwise("other").alias("term_group"),
        F.lower(F.trim(F.col("term"))).alias("term_clean"),
    )
)

result = (
    annotated
    .select(
        "id",
        "course_id",
        "term",
        "score",
        "enrolled_at",
        "avg_score_minus_avg",
        "score_minus_avg_minus_avg",
        "score_minus_avg_cume_dist",
        "first_enrollment_enrolled_at",
        "enrolled_at_quarter",
        "term_group",
        "term_clean",
        F.round(F.col("avg_score") / F.nullif(F.col("score"), F.lit(0)), 3).alias("avg_score_per_score"),
    )
    .orderBy(F.col("score_minus_avg_minus_avg").desc())
)
