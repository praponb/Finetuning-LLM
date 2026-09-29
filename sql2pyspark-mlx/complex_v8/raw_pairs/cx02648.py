from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_term_len = Window.partitionBy("term_len")
w_grade_status_term_len_by_sco = Window.partitionBy("grade_status", "term_len").orderBy("score", "id")
w_student_id_grade_status_by_t = Window.partitionBy("student_id", "grade_status").orderBy("total_enrollment_score_minus_avg", "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)

scoped_enrollments = (
    df_enrollments
    .filter((F.col("course_id") >= 3) & (F.col("score") <= 83.1))
    .select(
        "id",
        "student_id",
        "course_id",
        "term",
        "score",
        "grade_status",
        F.date_format(F.col("enrolled_at"), "yyyy-MM").alias("enrolled_at_label"),
        F.datediff(F.lit("2025-12-31").cast("date"), F.col("enrolled_at")).alias("days_since_enrolled_at"),
        F.length(F.trim(F.col("term"))).alias("term_len"),
    )
)

enrollment_stats = (
    df_enrollments
    .groupBy("student_id")
    .agg(F.sum("score").alias("total_enrollment_score"))
)

summarized_rows = (
    scoped_enrollments.alias("s")
    .join(enrollment_stats.alias("e"), F.col("e.student_id") == F.col("s.student_id"), "left")
    .select("s.*", "e.total_enrollment_score")
)

term_stats = (
    summarized_rows
    .groupBy("term")
    .agg(F.avg("total_enrollment_score").alias("avg_total_enrollment_score"))
)

outliers = (
    summarized_rows.alias("s")
    .join(term_stats.alias("g"), F.col("g.term") == F.col("s.term"), "inner")
    .filter(F.col("s.total_enrollment_score") > F.col("g.avg_total_enrollment_score"))
    .select(
        "s.*",
        "g.avg_total_enrollment_score",
        F.round(F.col("s.total_enrollment_score") - F.col("g.avg_total_enrollment_score"), 2).alias("total_enrollment_score_minus_avg"),
    )
)

labeled = (
    outliers
    .select(
        "*",
        F.concat(F.col("term"), F.lit("#"), F.col("id").cast("string")).alias("term_ref"),
        F.round(F.col("score") * 0.9, 2).alias("score_adj"),
    )
)

ranked = (
    labeled
    .select(
        "id",
        "student_id",
        "term",
        "enrolled_at_label",
        "days_since_enrolled_at",
        "total_enrollment_score",
        "avg_total_enrollment_score",
        "term_ref",
        "score_adj",
        F.count(F.lit(1)).over(w_term_len).alias("rows_in_term_len"),
        F.round(F.col("term_len") / F.nullif(F.sum("term_len").over(w_term_len), F.lit(0)), 4).alias("term_len_share_of_term_len"),
        F.rank().over(w_grade_status_term_len_by_sco).alias("score_rank"),
        F.first("score").over(w_student_id_grade_status_by_t).alias("first_score"),
    )
)

enrollment_stats_2 = (
    df_enrollments
    .groupBy("student_id")
    .agg(F.min("score").alias("min_enrollment_score"))
)

profiled = (
    ranked.alias("s")
    .join(enrollment_stats_2.alias("e"), F.col("e.student_id") == F.col("s.student_id"), "left")
    .select("s.*", "e.min_enrollment_score")
)

enrollment_stats_3 = (
    df_enrollments
    .groupBy("student_id")
    .agg(
        F.min("score").alias("min_enrollment_score_2"),
        F.min("enrolled_at").alias("first_enrollment_enrolled_at"),
        F.min("score").alias("min_enrollment_score_3"),
    )
)

profiled_2 = (
    profiled.alias("s")
    .join(enrollment_stats_3.alias("e"), F.col("e.student_id") == F.col("s.student_id"), "left")
    .select(
        "s.*",
        "e.min_enrollment_score_2",
        "e.first_enrollment_enrolled_at",
        "e.min_enrollment_score_3",
    )
)

result = (
    profiled_2
    .select(
        "id",
        "student_id",
        "term",
        "enrolled_at_label",
        "days_since_enrolled_at",
        "total_enrollment_score",
        "term_ref",
        "rows_in_term_len",
        "term_len_share_of_term_len",
        "score_rank",
        "first_score",
        "first_enrollment_enrolled_at",
        "min_enrollment_score_3",
        F.when(F.col("term").isin("2025T2", "2024T2"), "group_a").when(F.col("term") == "2025T1", "group_b").otherwise("other").alias("term_group"),
        (F.floor(F.col("first_score") / 25) * 25).alias("first_score_bucket"),
    )
    .orderBy(
        F.col("rows_in_term_len").desc_nulls_first(),
        F.col("enrolled_at_label").desc(),
    )
)
