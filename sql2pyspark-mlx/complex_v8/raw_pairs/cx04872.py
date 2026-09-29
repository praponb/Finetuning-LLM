from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_all_by_enrolled_at_ties = Window.orderBy(F.col("enrolled_at").desc())
w_all_by_enrolled_at = Window.orderBy(F.col("enrolled_at").desc(), "id")
w_all_by_enrolled_at_prior_6 = Window.orderBy(F.col("enrolled_at").desc(), "id").rowsBetween(-6, -1)
w_term = Window.partitionBy("term")
w_grade_status_student_id_by_e = Window.partitionBy("grade_status", "student_id").orderBy(F.col("enrolled_at").desc_nulls_first(), "id")
w_all_by_enrolled_at_full = Window.orderBy(F.col("enrolled_at").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_all_by_enrolled_at_prior_2 = Window.orderBy(F.col("enrolled_at").desc(), "id").rowsBetween(-2, -1)
w_grade_status_student_id_by_e_2 = Window.partitionBy("grade_status", "student_id").orderBy(F.col("enrolled_at").desc_nulls_first(), "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_grade_status_course_id = Window.partitionBy("grade_status", "course_id")
w_distinct_student_id_in_term = Window.partitionBy("distinct_student_id_in_term", "student_id")
w_term_distinct_course_id_in_t = Window.partitionBy("term", "distinct_course_id_in_term").orderBy(F.col("running_score").asc_nulls_last())

filtered = (
    df_enrollments
    .filter(F.col("student_id") <= 17)
    .select(
        "id",
        "student_id",
        "course_id",
        "term",
        "score",
        "grade_status",
        "enrolled_at",
        F.round(F.col("score") * 1.07, 2).alias("score_adj"),
        F.concat(F.col("grade_status"), F.lit("#"), F.col("id").cast("string")).alias("grade_status_ref"),
    )
)

ranked = (
    filtered
    .select(
        "id",
        "student_id",
        "course_id",
        "term",
        "score",
        "grade_status",
        "score_adj",
        "grade_status_ref",
        F.dense_rank().over(w_all_by_enrolled_at_ties).alias("enrolled_at_dense_rank"),
        F.round((F.unix_timestamp("enrolled_at") - F.unix_timestamp(F.lag("enrolled_at").over(w_all_by_enrolled_at))) / 60, 1).alias("minutes_since_prev_enrolled_at"),
        F.round(F.avg("score").over(w_all_by_enrolled_at_prior_6), 2).alias("score_avg_6_prior"),
        F.round(F.percent_rank().over(w_all_by_enrolled_at), 3).alias("enrolled_at_percent_rank"),
        F.round(F.col("score_adj") / F.nullif(F.sum("score_adj").over(w_term), F.lit(0)), 4).alias("score_adj_share_of_term"),
        (F.col("score") - F.lag("score").over(w_grade_status_student_id_by_e)).alias("score_change"),
        F.first("enrolled_at").over(w_all_by_enrolled_at_full).alias("first_enrolled_at"),
        F.sum("score").over(w_all_by_enrolled_at_prior_2).alias("score_sum_2_prior"),
        F.count(F.lit(1)).over(w_term).alias("rows_in_term"),
        F.sum("score").over(w_grade_status_student_id_by_e_2).alias("running_score"),
        F.size(F.collect_set("student_id").over(w_term)).alias("distinct_student_id_in_term"),
        F.rank().over(w_grade_status_student_id_by_e).alias("enrolled_at_rank"),
        F.size(F.collect_set("course_id").over(w_term)).alias("distinct_course_id_in_term"),
    )
)

top_per_group = ranked.filter(F.col("enrolled_at_dense_rank") <= 3)

metrics = (
    top_per_group
    .select(
        "*",
        F.round(F.col("enrolled_at_percent_rank") / F.nullif(F.sum("enrolled_at_percent_rank").over(w_grade_status_course_id), F.lit(0)), 4).alias("enrolled_at_percent_rank_share_of_grade_status_c"),
        F.round(F.col("score_adj") - F.avg("score_adj").over(w_distinct_student_id_in_term), 2).alias("score_adj_vs_distinct_student_id_in_term_student"),
        F.dense_rank().over(w_term_distinct_course_id_in_t).alias("running_score_dense_rank"),
    )
)

result = (
    metrics
    .select(
        "id",
        "student_id",
        "course_id",
        "score",
        "score_adj",
        "grade_status_ref",
        "enrolled_at_dense_rank",
        "minutes_since_prev_enrolled_at",
        "score_avg_6_prior",
        "enrolled_at_percent_rank",
        "score_adj_share_of_term",
        "score_change",
        "first_enrolled_at",
        "rows_in_term",
        "running_score",
        "enrolled_at_rank",
        "distinct_course_id_in_term",
        "enrolled_at_percent_rank_share_of_grade_status_c",
        "score_adj_vs_distinct_student_id_in_term_student",
        "running_score_dense_rank",
    )
    .orderBy(
        F.col("id").desc(),
        F.col("distinct_course_id_in_term").desc_nulls_first(),
    )
)
