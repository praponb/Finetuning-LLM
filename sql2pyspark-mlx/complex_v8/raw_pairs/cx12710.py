from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_all_by_score_filled_prior_1 = Window.orderBy(F.col("score_filled").desc(), "id").rowsBetween(-1, -1)
w_term = Window.partitionBy("term")
w_course_id_grade_status_by_sc = Window.partitionBy("course_id", "grade_status").orderBy(F.col("score_bucket").asc_nulls_last())
w_course_id_grade_status_by_sc_2 = Window.partitionBy("course_id", "grade_status").orderBy(F.col("score_bucket").asc_nulls_last(), "id")
w_grade_status_by_score_bucket = Window.partitionBy("grade_status").orderBy("score_bucket_percent_rank", "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)

filtered_enrollments = (
    df_enrollments
    .filter((F.col("term") == "2025T1") & (F.col("grade_status") == "WITHDRAWN"))
    .select(
        "id",
        "student_id",
        "course_id",
        "term",
        "grade_status",
        "enrolled_at",
        (F.floor(F.col("score") / 25) * 25).alias("score_bucket"),
        F.lower(F.regexp_replace(F.trim(F.col("term")), " ", "_")).alias("term_slug"),
        F.coalesce(F.col("score"), F.lit(0.0)).alias("score_filled"),
    )
)

metrics = (
    filtered_enrollments
    .select(
        "id",
        "term",
        "grade_status",
        "enrolled_at",
        "score_bucket",
        "term_slug",
        "score_filled",
        F.round(F.avg("score_bucket").over(w_all_by_score_filled_prior_1), 2).alias("score_bucket_avg_1_prior"),
        F.round(F.col("score_filled") - F.avg("score_filled").over(w_term), 2).alias("score_filled_vs_term_avg"),
        F.round(F.percent_rank().over(w_course_id_grade_status_by_sc), 3).alias("score_bucket_percent_rank"),
        F.count(F.lit(1)).over(w_term).alias("rows_in_term"),
        F.ntile(3).over(w_course_id_grade_status_by_sc_2).alias("score_bucket_tercile"),
    )
)

term_stats = (
    metrics
    .groupBy("term")
    .agg(F.avg("rows_in_term").alias("avg_rows_in_term"))
)

vs_group = (
    metrics.alias("s")
    .join(term_stats.alias("g"), F.col("g.term") == F.col("s.term"), "inner")
    .filter(F.col("s.rows_in_term") > F.col("g.avg_rows_in_term"))
    .select(
        "s.*",
        "g.avg_rows_in_term",
        F.round(F.col("s.rows_in_term") - F.col("g.avg_rows_in_term"), 2).alias("rows_in_term_minus_avg"),
    )
)

sequenced = (
    vs_group
    .select(
        "*",
        F.nth_value("rows_in_term_minus_avg", 2).over(w_grade_status_by_score_bucket).alias("second_rows_in_term_minus_avg"),
        F.max("score_bucket_avg_1_prior").over(w_term).alias("term_max_score_bucket_avg_1_prior"),
        F.min("score_bucket_tercile").over(w_term).alias("term_min_score_bucket_tercile"),
    )
)

annotated = (
    sequenced
    .filter(F.col("grade_status").isin("WITHDRAWN", "PASS"))
    .select(
        "id",
        "grade_status",
        "enrolled_at",
        "score_filled",
        "score_bucket_avg_1_prior",
        "score_filled_vs_term_avg",
        "score_bucket_percent_rank",
        "rows_in_term",
        "score_bucket_tercile",
        "avg_rows_in_term",
        "rows_in_term_minus_avg",
        "second_rows_in_term_minus_avg",
        "term_max_score_bucket_avg_1_prior",
        "term_min_score_bucket_tercile",
        F.date_format(F.col("enrolled_at"), "yyyy-MM-dd HH:00").alias("enrolled_at_label"),
        F.least(F.col("rows_in_term"), F.col("score_bucket_tercile")).alias("least_rows_in_term_score_bucket_tercile"),
        F.greatest(F.col("rows_in_term_minus_avg"), F.col("score_filled_vs_term_avg")).alias("greatest_rows_in_term_minus_avg_score_filled_vs_"),
    )
)

filtered = (
    annotated
    .crossJoin(annotated.agg(F.avg("term_max_score_bucket_avg_1_prior").alias("overall_avg_term_max_score_bucket_avg_1_prior")))
    .filter(F.col("term_max_score_bucket_avg_1_prior") > F.col("overall_avg_term_max_score_bucket_avg_1_prior"))
    .drop("overall_avg_term_max_score_bucket_avg_1_prior")
)

result = (
    filtered
    .select(
        "id",
        "grade_status",
        "enrolled_at",
        "score_filled",
        "score_bucket_avg_1_prior",
        "score_bucket_percent_rank",
        "score_bucket_tercile",
        "avg_rows_in_term",
        "rows_in_term_minus_avg",
        "term_max_score_bucket_avg_1_prior",
        "least_rows_in_term_score_bucket_tercile",
        "greatest_rows_in_term_minus_avg_score_filled_vs_",
    )
    .orderBy(
        F.col("greatest_rows_in_term_minus_avg_score_filled_vs_").desc(),
        "avg_rows_in_term",
    )
)
