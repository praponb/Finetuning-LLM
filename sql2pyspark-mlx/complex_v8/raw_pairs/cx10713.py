from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_term_distinct_student_ids_by = Window.partitionBy("term", "distinct_student_ids").orderBy(F.col("distinct_grade_status").desc())
w_term_distinct_grade_status = Window.partitionBy("term", "distinct_grade_status")

prepared_enrollments = (
    df_enrollments
    .filter((F.col("course_id") <= 10) | (F.col("student_id").isNotNull()))
    .select(
        "id",
        "student_id",
        "course_id",
        "term",
        "score",
        "grade_status",
        "enrolled_at",
        F.date_format(F.col("enrolled_at"), "yyyy-MM-dd HH:00").alias("enrolled_at_label"),
    )
)

rollup_base = (
    prepared_enrollments
    .groupBy("term")
    .agg(
        F.round(F.sum("score"), 2).alias("total_score"),
        F.countDistinct("student_id").alias("distinct_student_ids"),
        F.min("score").alias("min_score"),
        F.round(F.stddev("score"), 2).alias("stddev_score"),
        F.countDistinct("grade_status").alias("distinct_grade_status"),
        F.sum("score").alias("sum_score_check"),
    )
    .filter(F.col("sum_score_check") > 32.7)
    .drop("sum_score_check")
)

with_windows = (
    rollup_base
    .select(
        "*",
        F.dense_rank().over(w_term_distinct_student_ids_by).alias("distinct_grade_status_dense_rank"),
        F.count(F.lit(1)).over(w_term_distinct_grade_status).alias("rows_in_term_distinct_grade_status"),
    )
)

qualified = (
    with_windows
    .crossJoin(with_windows.agg(F.max("distinct_grade_status").alias("overall_max_distinct_grade_status")))
    .filter(F.col("distinct_grade_status") < F.col("overall_max_distinct_grade_status"))
    .drop("overall_max_distinct_grade_status")
)

scored = (
    qualified
    .select(
        "*",
        F.length(F.trim(F.col("term"))).alias("term_len"),
        (F.col("distinct_student_ids") + F.col("total_score")).alias("distinct_student_ids_plus_total_score"),
        F.least(F.col("stddev_score"), F.col("total_score")).alias("least_stddev_score_total_score"),
    )
)

tagged = (
    scored
    .select(
        "term",
        "total_score",
        "min_score",
        "stddev_score",
        "distinct_grade_status_dense_rank",
        "rows_in_term_distinct_grade_status",
        "term_len",
        "distinct_student_ids_plus_total_score",
        F.coalesce(F.col("term"), F.lit("UNKNOWN")).alias("term_or_unknown"),
        F.coalesce(F.col("total_score"), F.lit(0.0)).alias("total_score_filled"),
        F.round(F.col("total_score") * 0.25, 2).alias("total_score_adj"),
    )
)

result = (
    tagged
    .select(
        "term",
        "min_score",
        "distinct_grade_status_dense_rank",
        "rows_in_term_distinct_grade_status",
        "term_len",
        "distinct_student_ids_plus_total_score",
        "term_or_unknown",
        "total_score_filled",
        "total_score_adj",
        (F.floor(F.col("total_score_filled") / 100) * 100).alias("total_score_filled_bucket"),
        F.lower(F.regexp_replace(F.trim(F.col("term_or_unknown")), " ", "_")).alias("term_or_unknown_slug"),
    )
    .orderBy(
        F.col("rows_in_term_distinct_grade_status").desc(),
        F.col("term_or_unknown").desc_nulls_first(),
    )
)
