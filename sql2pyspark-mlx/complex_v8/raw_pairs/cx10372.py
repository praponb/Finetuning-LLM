from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_distinct_grade_status_studen = Window.partitionBy("distinct_grade_status", "student_id")
w_all_by_max_score = Window.orderBy("max_score", "grade_status", "student_id", "course_id")
w_all_by_max_score_ties = Window.orderBy("max_score")

cleaned = (
    df_enrollments
    .filter(F.col("student_id") >= 4)
    .select(
        "id",
        "student_id",
        "course_id",
        "term",
        "score",
        "grade_status",
        F.concat_ws(": ", F.col("term"), F.col("grade_status")).alias("term_grade_status_label"),
        F.date_trunc("quarter", F.col("enrolled_at")).alias("enrolled_at_quarter"),
        F.when(F.col("score") >= 57.8, "large").when(F.col("score") >= 38.3, "medium").otherwise("small").alias("score_tier"),
    )
)

enriched = (
    cleaned
    .filter(F.to_date(F.col("enrolled_at_quarter")) < F.lit("2025-02-09").cast("date"))
    .select(
        "*",
        F.substring(F.trim(F.col("term_grade_status_label")), 1, 4).alias("term_grade_status_label_prefix"),
    )
)

grade_status_stats = (
    enriched
    .groupBy("grade_status")
    .agg(F.avg("score").alias("avg_score"))
)

compared = (
    enriched.alias("s")
    .join(grade_status_stats.alias("g"), F.col("g.grade_status") == F.col("s.grade_status"), "inner")
    .filter(F.col("s.score") > F.col("g.avg_score"))
    .select(
        "s.*",
        "g.avg_score",
        F.round(F.col("s.score") - F.col("g.avg_score"), 2).alias("score_minus_avg"),
    )
)

grade_status_stats_2 = (
    compared
    .groupBy("grade_status")
    .agg(F.avg("avg_score").alias("avg_avg_score"))
)

outliers = (
    compared.alias("s")
    .join(grade_status_stats_2.alias("g"), F.col("g.grade_status") == F.col("s.grade_status"), "inner")
    .filter(F.col("s.avg_score") > F.col("g.avg_avg_score"))
    .select(
        "s.*",
        "g.avg_avg_score",
        F.round(F.col("s.avg_score") - F.col("g.avg_avg_score"), 2).alias("avg_score_minus_avg"),
    )
)

summary = (
    outliers
    .groupBy("grade_status", "student_id", "course_id")
    .agg(
        F.max("score").alias("max_score"),
        F.countDistinct("grade_status").alias("distinct_grade_status"),
        F.min("enrolled_at_quarter").alias("first_enrolled_at_quarter"),
        F.array_sort(F.collect_set("term")).alias("term_list"),
        F.sum("score_minus_avg").alias("sum_score_minus_avg_check"),
    )
    .filter(F.col("sum_score_minus_avg_check") > -30.0)
    .drop("sum_score_minus_avg_check")
)

metrics = (
    summary
    .select(
        "grade_status",
        "student_id",
        "course_id",
        "max_score",
        "first_enrolled_at_quarter",
        "term_list",
        F.round(F.col("max_score") / F.nullif(F.sum("max_score").over(w_distinct_grade_status_studen), F.lit(0)), 4).alias("max_score_share_of_distinct_grade_status_student"),
        F.max("distinct_grade_status").over(w_distinct_grade_status_studen).alias("distinct_grade_status_student_id_max_distinct_gr"),
        F.dense_rank().over(w_all_by_max_score).alias("max_score_dense_rank"),
        F.dense_rank().over(w_all_by_max_score_ties).alias("max_score_dense_rank_2"),
        F.size(F.collect_set("grade_status").over(w_distinct_grade_status_studen)).alias("distinct_grade_status_in_distinct_grade_status_s"),
    )
)

enrollment_stats = (
    df_enrollments
    .groupBy("student_id")
    .agg(
        F.round(F.avg("score"), 2).alias("avg_enrollment_score"),
        F.max("score").alias("max_enrollment_score"),
        F.count("*").alias("enrollment_count"),
    )
)

with_counts = (
    metrics.alias("s")
    .join(enrollment_stats.alias("e"), F.col("e.student_id") == F.col("s.student_id"), "left")
    .select(
        "s.*",
        "e.avg_enrollment_score",
        "e.max_enrollment_score",
        F.coalesce(F.col("e.enrollment_count"), F.lit(0)).alias("enrollment_count"),
    )
)

result = (
    with_counts
    .select(
        "grade_status",
        "student_id",
        "course_id",
        "max_score",
        "max_score_share_of_distinct_grade_status_student",
        "distinct_grade_status_student_id_max_distinct_gr",
        "max_score_dense_rank",
        "max_score_dense_rank_2",
        "distinct_grade_status_in_distinct_grade_status_s",
        "avg_enrollment_score",
        "max_enrollment_score",
        "enrollment_count",
        F.coalesce(F.col("grade_status"), F.lit("UNKNOWN")).alias("grade_status_or_unknown"),
        (F.col("enrollment_count") - F.col("max_score_dense_rank")).alias("enrollment_count_minus_max_score_dense_rank"),
    )
    .orderBy(
        F.col("max_score_share_of_distinct_grade_status_student").desc(),
        "grade_status",
        "student_id",
        "course_id",
    )
    .limit(20)
)
