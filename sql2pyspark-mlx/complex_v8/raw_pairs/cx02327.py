from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_grade_status_student_id = Window.partitionBy("grade_status", "student_id")
w_all_by_score_ties = Window.orderBy(F.col("score").desc_nulls_first())
w_student_id_term_by_score = Window.partitionBy("student_id", "term").orderBy("score", "id")
w_grade_status_term_by_enrolle = Window.partitionBy("grade_status", "term").orderBy("enrolled_at", "id")
w_grade_status_course_id = Window.partitionBy("grade_status", "course_id")
w_student_id_term_by_score_mov = Window.partitionBy("student_id", "term").orderBy("score", "id").rowsBetween(-1, Window.currentRow)
w_all_by_score_full = Window.orderBy(F.col("score").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_student_id_course_id_by_scor = Window.partitionBy("student_id", "course_id").orderBy("score")
w_all_by_score = Window.orderBy(F.col("score").desc(), "id")
w_all_by_score_2 = Window.orderBy(F.col("score").desc_nulls_first(), "id")
w_all_by_score_share_of_grade = Window.orderBy(F.col("score_share_of_grade_status_student_id").desc(), "id").rowsBetween(-2, Window.currentRow)
w_term = Window.partitionBy("term")
w_enrolled_at_month_enrolled_a = Window.partitionBy(F.date_trunc("month", "enrolled_at"), "enrolled_at_quartile").orderBy(F.col("grade_status_student_id_avg_score").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

scoped_enrollments = (
    df_enrollments
    .filter((F.col("term").isin("2024T2", "2025T1")) & (F.col("score") <= 74.9))
    .select(
        "id",
        "student_id",
        "course_id",
        "term",
        "score",
        "grade_status",
        "enrolled_at",
        F.concat_ws("|", F.col("term"), F.col("grade_status")).alias("term_grade_status_label"),
    )
)

ranked = (
    scoped_enrollments
    .select(
        "*",
        F.max("score").over(w_grade_status_student_id).alias("grade_status_student_id_max_score"),
        F.round(F.avg("score").over(w_grade_status_student_id), 2).alias("grade_status_student_id_avg_score"),
        F.round(F.cume_dist().over(w_all_by_score_ties), 3).alias("score_cume_dist"),
        F.round(F.percent_rank().over(w_student_id_term_by_score), 3).alias("score_percent_rank"),
        F.ntile(4).over(w_grade_status_term_by_enrolle).alias("enrolled_at_quartile"),
        F.sum("score").over(w_grade_status_course_id).alias("grade_status_course_id_sum_score"),
        F.round(F.col("score") - F.avg("score").over(w_grade_status_course_id), 2).alias("score_vs_grade_status_course_id_avg"),
        F.round(F.avg("score").over(w_student_id_term_by_score_mov), 2).alias("score_avg_2"),
        F.last("score").over(w_all_by_score_full).alias("last_score"),
        F.round(F.percent_rank().over(w_student_id_course_id_by_scor), 3).alias("score_percent_rank_2"),
        F.row_number().over(w_all_by_score).alias("rn"),
        F.size(F.collect_set("term").over(w_grade_status_student_id)).alias("distinct_term_in_grade_status_student_id"),
        F.round(F.col("score") / F.nullif(F.sum("score").over(w_grade_status_student_id), F.lit(0)), 4).alias("score_share_of_grade_status_student_id"),
        F.round((F.unix_timestamp("enrolled_at") - F.unix_timestamp(F.lag("enrolled_at").over(w_grade_status_term_by_enrolle))) / 60, 1).alias("minutes_since_prev_enrolled_at"),
        F.lead("enrolled_at").over(w_all_by_score_2).alias("next_enrolled_at"),
    )
)

top_rows = ranked.filter(F.col("rn") == 1)

windowed = (
    top_rows
    .select(
        "*",
        F.sum("score_cume_dist").over(w_all_by_score_share_of_grade).alias("score_cume_dist_sum_3"),
        F.round(F.col("score") - F.avg("score").over(w_term), 2).alias("score_vs_term_avg"),
        F.last("enrolled_at_quartile", ignorenulls=True).over(w_enrolled_at_month_enrolled_a).alias("last_known_enrolled_at_quartile"),
    )
)

result = (
    windowed
    .select(
        "id",
        "student_id",
        "course_id",
        "term",
        "score",
        "grade_status",
        "enrolled_at",
        "term_grade_status_label",
        "grade_status_student_id_max_score",
        "grade_status_student_id_avg_score",
        "score_percent_rank",
        "enrolled_at_quartile",
        "grade_status_course_id_sum_score",
        "score_avg_2",
        "score_percent_rank_2",
        "rn",
        "distinct_term_in_grade_status_student_id",
        "score_share_of_grade_status_student_id",
        "minutes_since_prev_enrolled_at",
        "next_enrolled_at",
        "score_vs_term_avg",
        "last_known_enrolled_at_quartile",
        (F.col("rn") - F.col("score_cume_dist_sum_3")).alias("rn_minus_score_cume_dist_sum_3"),
    )
    .orderBy(F.col("grade_status").desc_nulls_first(), "rn")
)
