from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_enrollment_id_by_score_adj = Window.partitionBy("enrollment_id").orderBy(F.col("score_adj").asc_nulls_last(), "id", "enrollment_id")
w_enrollment_id_by_score_adj_f = Window.partitionBy("enrollment_id").orderBy(F.col("score_adj").asc_nulls_last(), "id", "enrollment_id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)

filtered_enrollments = (
    df_enrollments
    .filter(F.col("student_id") >= 2)
    .select(
        "id",
        "student_id",
        "course_id",
        "term",
        "score",
        "grade_status",
        "enrolled_at",
        F.coalesce(F.col("term"), F.lit("UNKNOWN")).alias("term_or_unknown"),
        F.round(F.col("score") * 1.1, 2).alias("score_adj"),
    )
)

line_level = (
    filtered_enrollments.alias("s")
    .join(df_enrollments.alias("e"), F.col("e.course_id") == F.col("s.course_id"), "left")
    .select(
        "s.id",
        "s.student_id",
        "s.course_id",
        "s.score",
        "s.grade_status",
        "s.term_or_unknown",
        "s.score_adj",
        F.col("e.id").alias("enrollment_id"),
        F.col("e.score").alias("enrollment_score"),
        F.col("e.grade_status").alias("enrollment_grade_status"),
    )
)

derived = (
    line_level
    .select(
        "*",
        F.round(F.col("enrollment_score") * 0.25, 2).alias("enrollment_score_adj"),
    )
)

enriched = (
    derived
    .filter(F.col("course_id").isNotNull())
    .select(
        "*",
        F.when(F.col("enrollment_score") >= 69.6, "A").when(F.col("enrollment_score") >= 26.2, "B").otherwise("C").alias("enrollment_score_tier"),
        F.concat(F.col("enrollment_grade_status"), F.lit("#"), F.col("student_id").cast("string")).alias("enrollment_grade_status_ref"),
    )
)

windowed = (
    enriched
    .select(
        "id",
        "course_id",
        "score",
        "term_or_unknown",
        "score_adj",
        "enrollment_id",
        "enrollment_score",
        "enrollment_score_adj",
        "enrollment_score_tier",
        "enrollment_grade_status_ref",
        F.round(F.percent_rank().over(w_enrollment_id_by_score_adj), 3).alias("score_adj_percent_rank"),
        F.nth_value("score_adj", 2).over(w_enrollment_id_by_score_adj_f).alias("second_score_adj"),
    )
)

grouped = (
    windowed
    .groupBy("term_or_unknown")
    .agg(
        F.round(F.stddev("score_adj"), 2).alias("stddev_score_adj"),
        F.count("*").alias("n_rows"),
        F.countDistinct("term_or_unknown").alias("distinct_term_or_unknowns"),
        F.round(F.sum("second_score_adj"), 2).alias("total_second_score_adj"),
    )
)

result = (
    grouped
    .select("term_or_unknown", "total_second_score_adj")
    .orderBy(
        F.col("total_second_score_adj").desc_nulls_first(),
        F.col("term_or_unknown").desc(),
    )
)
