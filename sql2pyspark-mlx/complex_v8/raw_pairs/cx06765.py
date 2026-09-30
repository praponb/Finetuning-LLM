from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_term_or_unknown = Window.partitionBy("term_or_unknown")
w_all_by_score_adj_ties = Window.orderBy("score_adj")

scoped_enrollments = (
    df_enrollments
    .filter(F.col("enrolled_at") >= F.lit("2025-01-07 10:00:00").cast("timestamp"))
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

enrollment_stats = (
    df_enrollments
    .groupBy("course_id")
    .agg(F.max("score").alias("max_enrollment_score"))
)

summarized_rows = (
    scoped_enrollments.alias("s")
    .join(enrollment_stats.alias("e"), F.col("e.course_id") == F.col("s.course_id"), "left")
    .select("s.*", "e.max_enrollment_score")
)

scored = (
    summarized_rows
    .select(
        "id",
        "student_id",
        "course_id",
        "term",
        "score",
        "enrolled_at",
        F.concat(F.col("grade_status"), F.lit("#"), F.col("course_id").cast("string")).alias("grade_status_ref"),
        F.date_format(F.col("enrolled_at"), "yyyy-MM-dd HH:00").alias("enrolled_at_label"),
    )
)

scored_2 = (
    scored
    .filter(F.col("term").isNotNull())
    .select(
        "id",
        "student_id",
        "course_id",
        "term",
        "score",
        "grade_status_ref",
        "enrolled_at_label",
        F.date_format(F.col("enrolled_at"), "yyyy-MM").alias("enrolled_at_label_2"),
        F.date_format(F.col("enrolled_at"), "MMM yyyy").alias("enrolled_at_label_3"),
    )
)

derived = (
    scored_2
    .select(
        "*",
        F.coalesce(F.col("term"), F.lit("UNKNOWN")).alias("term_or_unknown"),
        F.concat(F.col("grade_status_ref"), F.lit("#"), F.col("student_id").cast("string")).alias("grade_status_ref_ref"),
        F.round(F.col("score") * 0.9, 2).alias("score_adj"),
    )
)

sequenced = (
    derived
    .select(
        "*",
        F.size(F.collect_set("term").over(w_term_or_unknown)).alias("distinct_term_in_term_or_unknown"),
        F.min("score_adj").over(w_term_or_unknown).alias("term_or_unknown_min_score_adj"),
        F.dense_rank().over(w_all_by_score_adj_ties).alias("score_adj_dense_rank"),
    )
)

derived_2 = (
    sequenced
    .filter(F.col("enrolled_at_label_2").isNotNull())
    .select(
        "*",
        F.concat(F.col("enrolled_at_label_2"), F.lit("#"), F.col("id").cast("string")).alias("enrolled_at_label_2_ref"),
        F.lower(F.regexp_replace(F.trim(F.col("enrolled_at_label")), " ", "_")).alias("enrolled_at_label_slug"),
        F.round(F.col("score") * 0.9, 2).alias("score_adj_2"),
    )
)

result = (
    derived_2
    .select(
        "id",
        "student_id",
        "course_id",
        "term",
        "grade_status_ref",
        "enrolled_at_label_2",
        "enrolled_at_label_3",
        "term_or_unknown",
        "grade_status_ref_ref",
        "term_or_unknown_min_score_adj",
        "score_adj_dense_rank",
        "enrolled_at_label_slug",
        "score_adj_2",
        F.round(F.col("score_adj_2") * 0.25, 2).alias("score_adj_2_adj"),
    )
    .orderBy(F.col("id").asc(), F.col("term_or_unknown").asc_nulls_last())
)
