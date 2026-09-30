from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_grade_status = Window.partitionBy("grade_status")
w_all_by_enrolled_at = Window.orderBy("enrolled_at", "id")
w_term = Window.partitionBy("term")
w_grade_status_by_enrolled_at = Window.partitionBy("grade_status").orderBy(F.col("enrolled_at").desc(), "id").rowsBetween(-3, Window.currentRow)
w_grade_status_by_score_prior = Window.partitionBy("grade_status").orderBy(F.col("score").desc(), "id").rowsBetween(-1, -1)
w_enrolled_at_year_term_by_sco = Window.partitionBy(F.year("enrolled_at"), "term").orderBy("score", "id")
w_course_id_by_score_ties = Window.partitionBy("course_id").orderBy("score")
w_grade_status_by_enrolled_at_2 = Window.partitionBy("grade_status").orderBy(F.col("enrolled_at").desc(), "id")
w_course_id_by_score_full = Window.partitionBy("course_id").orderBy("score", "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_grade_status_prev_enrolled_a = Window.partitionBy("grade_status", F.date_trunc("month", "prev_enrolled_at"))
w_enrolled_at_month_grade_stat = Window.partitionBy(F.date_trunc("month", "enrolled_at"), "grade_status").orderBy("next3_score", "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)

recent = (
    df_enrollments
    .select(
        "id",
        "student_id",
        "course_id",
        "term",
        "score",
        "grade_status",
        "enrolled_at",
        F.coalesce(F.col("term"), F.lit("UNKNOWN")).alias("term_or_unknown"),
    )
)

windowed = (
    recent
    .select(
        "*",
        F.round(F.col("score") / F.nullif(F.sum("score").over(w_grade_status), F.lit(0)), 4).alias("score_share_of_grade_status"),
        F.round(F.percent_rank().over(w_all_by_enrolled_at), 3).alias("enrolled_at_percent_rank"),
        F.round(F.col("score") - F.avg("score").over(w_term), 2).alias("score_vs_term_avg"),
        F.sum("score").over(w_grade_status_by_enrolled_at).alias("score_sum_4"),
        F.round(F.avg("score").over(w_grade_status_by_score_prior), 2).alias("score_avg_1_prior"),
        F.lag("enrolled_at").over(w_enrolled_at_year_term_by_sco).alias("prev_enrolled_at"),
        F.round(F.percent_rank().over(w_course_id_by_score_ties), 3).alias("score_percent_rank"),
        F.lead("score", 3, 0).over(w_grade_status_by_enrolled_at_2).alias("next3_score"),
        F.first("score").over(w_course_id_by_score_full).alias("first_score"),
    )
)

ranked = (
    windowed
    .select(
        "id",
        "student_id",
        "term",
        "score",
        "term_or_unknown",
        "score_share_of_grade_status",
        "enrolled_at_percent_rank",
        "score_sum_4",
        "score_avg_1_prior",
        "prev_enrolled_at",
        "next3_score",
        "first_score",
        F.size(F.collect_set("term_or_unknown").over(w_grade_status)).alias("distinct_term_or_unknown_in_grade_status"),
        F.size(F.collect_set("course_id").over(w_grade_status_prev_enrolled_a)).alias("distinct_course_id_in_grade_status_prev_enrolled"),
        F.first("score_vs_term_avg").over(w_enrolled_at_month_grade_stat).alias("first_score_vs_term_avg"),
        F.round(F.avg("score_avg_1_prior").over(w_grade_status_prev_enrolled_a), 2).alias("grade_status_prev_enrolled_at_month_avg_score_av"),
    )
)

result = (
    ranked
    .select(
        "id",
        "student_id",
        "term",
        "score",
        "term_or_unknown",
        "score_share_of_grade_status",
        "score_avg_1_prior",
        "next3_score",
        "first_score",
        "distinct_course_id_in_grade_status_prev_enrolled",
        "first_score_vs_term_avg",
        "grade_status_prev_enrolled_at_month_avg_score_av",
        F.coalesce(F.col("score"), F.lit(0.0)).alias("score_filled"),
        F.concat_ws(" / ", F.col("term"), F.col("term_or_unknown")).alias("term_term_or_unknown_label"),
    )
    .orderBy(
        F.col("first_score_vs_term_avg").desc_nulls_first(),
        F.col("first_score").asc(),
        "id",
    )
    .limit(10)
)
