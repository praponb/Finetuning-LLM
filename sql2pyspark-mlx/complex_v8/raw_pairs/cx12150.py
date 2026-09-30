from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_course_id_by_term_len_full = Window.partitionBy("course_id").orderBy(F.col("term_len").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_term = Window.partitionBy("term")
w_all_by_term_len = Window.orderBy("term_len", "id")
w_all_by_score = Window.orderBy("score", "id")
w_course_id_by_term_len = Window.partitionBy("course_id").orderBy(F.col("term_len").desc(), "id")
w_all_by_term_len_ties = Window.orderBy("term_len")
w_all_by_term_len_prior_6 = Window.orderBy("term_len", "id").rowsBetween(-6, -1)
w_prev_term_len_by_prev_term_l = Window.partitionBy("prev_term_len").orderBy(F.col("prev_term_len").asc_nulls_last(), "id")
w_prev_term_len_by_prev_term_l_2 = Window.partitionBy("prev_term_len").orderBy(F.col("prev_term_len").asc_nulls_last(), "id").rowsBetween(-2, -1)
w_prev_term_len_by_prev_term_l_3 = Window.partitionBy("prev_term_len").orderBy(F.col("prev_term_len").asc_nulls_last(), "id").rowsBetween(-3, Window.currentRow)

recent_enrollments = (
    df_enrollments
    .filter((F.col("term") != "2025T2") & (F.month(F.col("enrolled_at")).isin(1, 3)))
    .select(
        "id",
        "student_id",
        "course_id",
        "term",
        "score",
        "enrolled_at",
        F.length(F.trim(F.col("term"))).alias("term_len"),
    )
)

ranked = (
    recent_enrollments
    .select(
        "id",
        "student_id",
        "course_id",
        "term",
        "score",
        "enrolled_at",
        "term_len",
        F.nth_value("score", 2).over(w_course_id_by_term_len_full).alias("second_score"),
        F.min("term_len").over(w_term).alias("term_min_term_len"),
        F.ntile(10).over(w_all_by_term_len).alias("term_len_decile"),
        F.row_number().over(w_all_by_score).alias("seq_no"),
        F.dense_rank().over(w_course_id_by_term_len).alias("term_len_dense_rank"),
        F.lag("term_len").over(w_all_by_score).alias("prev_term_len"),
        F.ntile(4).over(w_course_id_by_term_len).alias("term_len_quartile"),
        F.count(F.lit(1)).over(w_term).alias("rows_in_term"),
        F.lead("enrolled_at").over(w_all_by_score).alias("next_enrolled_at"),
        F.size(F.collect_set("student_id").over(w_term)).alias("distinct_student_id_in_term"),
        F.round(F.col("score") - F.avg("score").over(w_term), 2).alias("score_vs_term_avg"),
        F.rank().over(w_all_by_term_len_ties).alias("term_len_rank"),
        F.round(F.col("term_len") - F.avg("term_len").over(w_term), 2).alias("term_len_vs_term_avg"),
        F.size(F.collect_set("course_id").over(w_term)).alias("distinct_course_id_in_term"),
        F.round(F.avg("term_len").over(w_all_by_term_len_prior_6), 2).alias("term_len_avg_6_prior"),
    )
)

leaders = ranked.filter(F.col("seq_no") <= 5)

sequenced = (
    leaders
    .select(
        "id",
        "student_id",
        "course_id",
        "score",
        "enrolled_at",
        "term_len",
        "second_score",
        "term_min_term_len",
        "seq_no",
        "prev_term_len",
        "term_len_quartile",
        "next_enrolled_at",
        "distinct_student_id_in_term",
        "score_vs_term_avg",
        "term_len_rank",
        "term_len_vs_term_avg",
        "distinct_course_id_in_term",
        F.rank().over(w_prev_term_len_by_prev_term_l).alias("prev_term_len_rank"),
        F.row_number().over(w_prev_term_len_by_prev_term_l).alias("seq_no_2"),
        F.sum("score_vs_term_avg").over(w_prev_term_len_by_prev_term_l_2).alias("score_vs_term_avg_sum_2_prior"),
        F.round(F.avg("term_len_quartile").over(w_prev_term_len_by_prev_term_l_3), 2).alias("term_len_quartile_avg_4"),
    )
)

result = (
    sequenced
    .select(
        "id",
        "student_id",
        "score",
        "term_len",
        "term_min_term_len",
        "seq_no",
        "prev_term_len",
        "term_len_quartile",
        "next_enrolled_at",
        "distinct_student_id_in_term",
        "score_vs_term_avg",
        "term_len_rank",
        "term_len_vs_term_avg",
        "distinct_course_id_in_term",
        "seq_no_2",
        "score_vs_term_avg_sum_2_prior",
        F.datediff(F.lit("2026-01-01").cast("date"), F.col("enrolled_at")).alias("days_since_enrolled_at"),
        (F.col("term_len_quartile") + F.col("term_len_vs_term_avg")).alias("term_len_quartile_plus_term_len_vs_term_avg"),
    )
    .orderBy(F.col("term_len_quartile_plus_term_len_vs_term_avg").desc(), "id")
    .limit(20)
)
