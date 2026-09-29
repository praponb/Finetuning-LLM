from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_course_id_by_score_distance = Window.partitionBy("course_id").orderBy("score_distance")
w_all_by_score_distance_full = Window.orderBy(F.col("score_distance").desc_nulls_first(), "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_term = Window.partitionBy("term")
w_all_by_score_distance = Window.orderBy(F.col("score_distance").desc_nulls_first(), "id")
w_student_id_term_by_score_dis = Window.partitionBy("student_id", "term").orderBy(F.col("score_distance").desc(), "id")
w_course_id_by_score_distance_2 = Window.partitionBy("course_id").orderBy("score_distance", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_course_id_by_score_distance_3 = Window.partitionBy("course_id").orderBy("score_distance", "id")
w_all_by_score_distance_ties = Window.orderBy(F.col("score_distance").desc_nulls_first())
w_course_id_score_distance_den = Window.partitionBy("course_id", "score_distance_dense_rank").orderBy(F.col("score_distance_share_of_term").desc())
w_student_id_grade_status_by_s = Window.partitionBy("student_id", "grade_status").orderBy(F.col("score_distance_cume_dist").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_score_distance_dense_rank_st = Window.partitionBy("score_distance_dense_rank", "student_id")
w_student_id_grade_status_by_r = Window.partitionBy("student_id", "grade_status").orderBy(F.col("rows_in_term").asc_nulls_last(), "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_grade_status = Window.partitionBy("grade_status")
w_student_id_grade_status_by_s_2 = Window.partitionBy("student_id", "grade_status").orderBy(F.col("score_distance_cume_dist").desc())

cleaned = (
    df_enrollments
    .filter(F.col("grade_status").isin("PASS", "INCOMPLETE"))
    .select(
        "id",
        "student_id",
        "course_id",
        "term",
        "score",
        "grade_status",
        F.abs(F.col("score") - 39.4).alias("score_distance"),
    )
)

with_trends = (
    cleaned
    .select(
        "id",
        "student_id",
        "course_id",
        "grade_status",
        "score_distance",
        F.dense_rank().over(w_course_id_by_score_distance).alias("score_distance_dense_rank"),
        F.nth_value("score_distance", 2).over(w_all_by_score_distance_full).alias("second_score_distance"),
        F.round(F.col("score") - F.avg("score").over(w_term), 2).alias("score_vs_term_avg"),
        F.round(F.col("score_distance") - F.avg("score_distance").over(w_term), 2).alias("score_distance_vs_term_avg"),
        F.round(F.col("score_distance") / F.nullif(F.sum("score_distance").over(w_term), F.lit(0)), 4).alias("score_distance_share_of_term"),
        F.count(F.lit(1)).over(w_term).alias("rows_in_term"),
        (F.col("score_distance") - F.lag("score_distance").over(w_all_by_score_distance)).alias("score_distance_change"),
        F.lag("score_distance").over(w_student_id_term_by_score_dis).alias("prev_score_distance"),
        F.first("score", ignorenulls=True).over(w_course_id_by_score_distance_2).alias("first_known_score"),
        F.round(F.col("score") / F.nullif(F.sum("score").over(w_term), F.lit(0)), 4).alias("score_share_of_term"),
        F.lead("score").over(w_course_id_by_score_distance_3).alias("next_score"),
        F.lag("score_distance", 1, 0).over(w_all_by_score_distance).alias("prev_score_distance_2"),
        F.round(F.cume_dist().over(w_all_by_score_distance_ties), 3).alias("score_distance_cume_dist"),
    )
)

first_rows = with_trends.filter(F.col("score_distance_dense_rank") <= 3)

with_windows = (
    first_rows
    .select(
        "*",
        F.rank().over(w_course_id_score_distance_den).alias("score_distance_share_of_term_rank"),
        F.last("score_vs_term_avg").over(w_student_id_grade_status_by_s).alias("last_score_vs_term_avg"),
        F.round(F.avg("prev_score_distance_2").over(w_score_distance_dense_rank_st), 2).alias("score_distance_dense_rank_student_id_avg_prev_sc"),
        F.last("score_distance_share_of_term", ignorenulls=True).over(w_student_id_grade_status_by_r).alias("last_known_score_distance_share_of_term"),
        F.count(F.lit(1)).over(w_grade_status).alias("rows_in_grade_status"),
        F.round(F.col("score_share_of_term") - F.avg("score_share_of_term").over(w_grade_status), 2).alias("score_share_of_term_vs_grade_status_avg"),
        F.round(F.percent_rank().over(w_student_id_grade_status_by_s_2), 3).alias("score_distance_cume_dist_percent_rank"),
    )
)

result = (
    with_windows
    .filter(F.col("score_distance_change").between(-40.0, 30.0))
    .select(
        "id",
        "course_id",
        "grade_status",
        "score_distance_dense_rank",
        "score_distance_share_of_term",
        "rows_in_term",
        "score_distance_change",
        "score_share_of_term",
        "next_score",
        "prev_score_distance_2",
        "score_distance_cume_dist",
        "score_distance_share_of_term_rank",
        "last_score_vs_term_avg",
        "score_distance_dense_rank_student_id_avg_prev_sc",
        "score_share_of_term_vs_grade_status_avg",
        "score_distance_cume_dist_percent_rank",
        F.round(F.col("score_distance") * 0.9, 2).alias("score_distance_adj"),
    )
    .orderBy(
        F.col("score_distance_share_of_term").asc(),
        F.col("score_share_of_term_vs_grade_status_avg").desc(),
    )
)
