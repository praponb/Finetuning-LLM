from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_grade_status_by_score_adj = Window.partitionBy("grade_status").orderBy("score_adj", "id")
w_grade_status_term_by_enrolle = Window.partitionBy("grade_status", "term").orderBy("enrolled_at", "id")
w_all_by_score = Window.orderBy(F.col("score").desc(), "id")
w_student_id = Window.partitionBy("student_id")
w_grade_status_by_score_adj_2 = Window.partitionBy("grade_status").orderBy(F.col("score_adj").desc(), "id")
w_grade_status_term_by_score_a = Window.partitionBy("grade_status", "term").orderBy("score_adj")
w_grade_status = Window.partitionBy("grade_status")
w_prev_enrolled_at_month_score = Window.partitionBy(F.month("prev_enrolled_at"), "score_quartile").orderBy("score_adj_rank", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_prev_enrolled_at_month_score_2 = Window.partitionBy(F.month("prev_enrolled_at"), "score_quartile").orderBy("score_adj_rank", "id")
w_all_by_score_adj_vs_grade_st = Window.orderBy("score_adj_vs_grade_status_avg", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_all_by_score_adj_vs_grade_st_2 = Window.orderBy("score_adj_vs_grade_status_avg", "id")

base_enrollments = (
    df_enrollments
    .filter(F.col("student_id").isNotNull())
    .select(
        "id",
        "student_id",
        "course_id",
        "term",
        "score",
        "grade_status",
        "enrolled_at",
        F.round(F.col("score") * 1.07, 2).alias("score_adj"),
    )
)

with_windows = (
    base_enrollments
    .select(
        "id",
        "student_id",
        "score",
        "grade_status",
        "enrolled_at",
        F.round((F.unix_timestamp("enrolled_at") - F.unix_timestamp(F.lag("enrolled_at").over(w_grade_status_by_score_adj))) / 60, 1).alias("minutes_since_prev_enrolled_at"),
        F.lag("enrolled_at").over(w_grade_status_term_by_enrolle).alias("prev_enrolled_at"),
        F.ntile(4).over(w_all_by_score).alias("score_quartile"),
        F.count(F.lit(1)).over(w_student_id).alias("rows_in_student_id"),
        F.round((F.unix_timestamp("enrolled_at") - F.unix_timestamp(F.lag("enrolled_at").over(w_grade_status_by_score_adj_2))) / 60, 1).alias("minutes_since_prev_enrolled_at_2"),
        F.size(F.collect_set("grade_status").over(w_student_id)).alias("distinct_grade_status_in_student_id"),
        F.rank().over(w_grade_status_term_by_score_a).alias("score_adj_rank"),
        F.round(F.col("score_adj") - F.avg("score_adj").over(w_grade_status), 2).alias("score_adj_vs_grade_status_avg"),
        F.round(F.cume_dist().over(w_grade_status_by_score_adj_2), 3).alias("score_adj_cume_dist"),
    )
)

ranked = (
    with_windows
    .select(
        "*",
        F.sum("score").over(w_prev_enrolled_at_month_score).alias("running_score"),
        F.rank().over(w_prev_enrolled_at_month_score_2).alias("score_adj_rank_rank"),
        F.round(F.col("score_adj_cume_dist") / F.nullif(F.sum("score_adj_cume_dist").over(w_student_id), F.lit(0)), 4).alias("score_adj_cume_dist_share_of_student_id"),
        F.sum("score").over(w_student_id).alias("student_id_sum_score"),
        F.first("score_adj_rank", ignorenulls=True).over(w_all_by_score_adj_vs_grade_st).alias("first_known_score_adj_rank"),
        F.lag("score", 3).over(w_all_by_score_adj_vs_grade_st_2).alias("prev3_score"),
        F.row_number().over(w_all_by_score_adj_vs_grade_st_2).alias("seq_no"),
    )
)

result = (
    ranked
    .filter(F.col("score_adj_rank_rank") <= 7)
    .select(
        "id",
        "student_id",
        "score",
        "enrolled_at",
        "minutes_since_prev_enrolled_at",
        "prev_enrolled_at",
        "score_quartile",
        "minutes_since_prev_enrolled_at_2",
        "distinct_grade_status_in_student_id",
        "score_adj_rank",
        "score_adj_vs_grade_status_avg",
        "score_adj_cume_dist",
        "running_score",
        "score_adj_rank_rank",
        "score_adj_cume_dist_share_of_student_id",
        "student_id_sum_score",
        "first_known_score_adj_rank",
        "seq_no",
        F.lower(F.regexp_replace(F.trim(F.col("grade_status")), " ", "_")).alias("grade_status_slug"),
        F.length(F.trim(F.col("grade_status"))).alias("grade_status_len"),
    )
    .orderBy("score", F.col("id").desc())
)
