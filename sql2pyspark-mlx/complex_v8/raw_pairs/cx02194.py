from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_all_by_avg_score_minus_avg_a = Window.orderBy(F.col("avg_score_minus_avg_adj").desc(), "term_len", "score_tier")
w_first_enrolled_at_year_to_da = Window.partitionBy(F.to_date("first_enrolled_at_year")).orderBy("total_score", "term_len", "score_tier").rowsBetween(-2, -1)
w_first_enrolled_at_year_to_da_2 = Window.partitionBy(F.to_date("first_enrolled_at_year")).orderBy("total_score", "term_len", "score_tier").rowsBetween(-6, Window.currentRow)
w_all_by_avg_score_minus_avg_a_2 = Window.orderBy(F.col("avg_score_minus_avg_adj").desc(), "term_len", "score_tier").rowsBetween(-3, -1)
w_term_len_score_tier = Window.partitionBy("term_len", "score_tier")
w_first_enrolled_at_year_month = Window.partitionBy(F.month("first_enrolled_at_year"), "score_tier")
w_all_by_max_score = Window.orderBy(F.col("max_score").desc(), "term_len", "score_tier")
w_all_by_max_score_moving_2 = Window.orderBy(F.col("max_score").desc(), "term_len", "score_tier").rowsBetween(-2, Window.currentRow)

cleaned = (
    df_enrollments
    .filter(F.col("student_id").isNotNull())
    .select(
        "id",
        "student_id",
        "course_id",
        "term",
        "score",
        "grade_status",
        F.concat(F.col("grade_status"), F.lit("#"), F.col("id").cast("string")).alias("grade_status_ref"),
        F.when(F.col("score") >= 73.7, "large").when(F.col("score") >= 39.4, "medium").otherwise("small").alias("score_tier"),
        F.date_trunc("year", F.col("enrolled_at")).alias("enrolled_at_year"),
    )
)

term_stats = (
    cleaned
    .groupBy("term")
    .agg(F.avg("score").alias("avg_score"))
)

vs_group = (
    cleaned.alias("s")
    .join(term_stats.alias("g"), F.col("g.term") == F.col("s.term"), "inner")
    .filter(F.col("s.score") > F.col("g.avg_score"))
    .select(
        "s.*",
        "g.avg_score",
        F.round(F.col("s.score") - F.col("g.avg_score"), 2).alias("score_minus_avg"),
    )
)

annotated = (
    vs_group
    .filter(F.col("score_minus_avg") > -30.0)
    .select(
        "id",
        "student_id",
        "course_id",
        "score",
        "grade_status_ref",
        "score_tier",
        "enrolled_at_year",
        "avg_score",
        F.length(F.trim(F.col("term"))).alias("term_len"),
        F.round(F.col("score_minus_avg") * 1.5, 2).alias("score_minus_avg_adj"),
        F.concat(F.col("grade_status_ref"), F.lit("#"), F.col("course_id").cast("string")).alias("grade_status_ref_ref"),
    )
)

summary = (
    annotated
    .groupBy("term_len", "score_tier")
    .agg(
        F.round(F.sum("score"), 2).alias("total_score"),
        F.min("enrolled_at_year").alias("first_enrolled_at_year"),
        F.max("score").alias("max_score"),
        F.round(F.avg("score_minus_avg_adj"), 2).alias("avg_score_minus_avg_adj"),
    )
)

windowed = (
    summary
    .select(
        "term_len",
        "score_tier",
        "total_score",
        "first_enrolled_at_year",
        "max_score",
        F.lead("max_score").over(w_all_by_avg_score_minus_avg_a).alias("next_max_score"),
        F.sum("max_score").over(w_first_enrolled_at_year_to_da).alias("max_score_sum_2_prior"),
        F.sum("total_score").over(w_first_enrolled_at_year_to_da_2).alias("total_score_sum_7"),
        F.round(F.avg("avg_score_minus_avg_adj").over(w_all_by_avg_score_minus_avg_a_2), 2).alias("avg_score_minus_avg_adj_avg_3_prior"),
        F.round(F.avg("term_len").over(w_term_len_score_tier), 2).alias("term_len_score_tier_avg_term_len"),
    )
)

screened = (
    windowed
    .crossJoin(windowed.agg(F.min("max_score").alias("overall_min_max_score")))
    .filter(F.col("max_score") > F.col("overall_min_max_score"))
    .drop("overall_min_max_score")
)

with_windows = (
    screened
    .select(
        "*",
        F.max("next_max_score").over(w_first_enrolled_at_year_month).alias("first_enrolled_at_year_month_score_tier_max_next"),
        F.row_number().over(w_all_by_max_score).alias("max_score_seq"),
        F.round(F.avg("total_score").over(w_all_by_max_score_moving_2), 2).alias("total_score_avg_3"),
        F.count(F.lit(1)).over(w_first_enrolled_at_year_month).alias("rows_in_first_enrolled_at_year_month_score_tier"),
        (F.col("total_score_sum_7") - F.lag("total_score_sum_7").over(w_all_by_max_score)).alias("total_score_sum_7_change"),
    )
)

result = (
    with_windows
    .select(
        "term_len",
        "total_score",
        "first_enrolled_at_year",
        "max_score",
        "next_max_score",
        "max_score_sum_2_prior",
        "total_score_sum_7",
        "first_enrolled_at_year_month_score_tier_max_next",
        "max_score_seq",
        "total_score_avg_3",
        "rows_in_first_enrolled_at_year_month_score_tier",
        F.round(F.col("total_score_sum_7_change") / F.nullif(F.col("term_len"), F.lit(0)), 3).alias("total_score_sum_7_change_per_term_len"),
    )
    .orderBy(F.col("max_score").desc())
)
