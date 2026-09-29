from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_student_id_term = Window.partitionBy("student_id", "term")
w_all_by_enrolled_at_full = Window.orderBy(F.col("enrolled_at").asc_nulls_last(), "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_enrolled_at_month_course_id = Window.partitionBy(F.date_trunc("month", "enrolled_at"), "course_id")
w_course_id_by_score_filled_ti = Window.partitionBy("course_id").orderBy("score_filled")
w_all_by_score_filled_running = Window.orderBy(F.col("score_filled").desc_nulls_first(), "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_term_student_id_by_score_fil = Window.partitionBy("term", "student_id").orderBy(F.col("score_filled").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_all_by_score_filled = Window.orderBy(F.col("score_filled").desc_nulls_first(), "id")
w_student_id_course_id_by_scor = Window.partitionBy("student_id", "course_id").orderBy(F.col("score_filled").desc())
w_term_student_id_by_score_fil_2 = Window.partitionBy("term", "student_id").orderBy(F.col("score_filled").desc(), "id").rowsBetween(-1, Window.currentRow)
w_course_id_by_score_filled = Window.partitionBy("course_id").orderBy("score_filled", "id")
w_score_filled_dense_rank_by_r = Window.partitionBy("score_filled_dense_rank").orderBy("running_score_filled", "id")
w_term_student_id = Window.partitionBy("term", "student_id")
w_score_filled_dense_rank_fill = Window.partitionBy("score_filled_dense_rank_filled", "distinct_grade_status_in_student_id_term").orderBy(F.col("score_filled_dense_rank").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_all_by_enrolled_at_month_cou = Window.orderBy("enrolled_at_month_course_id_sum_score_filled", "id")
w_course_id_by_enrolled_at_mon = Window.partitionBy("course_id").orderBy("enrolled_at_month_course_id_sum_score_filled", "id")

recent_enrollments = (
    df_enrollments
    .filter((F.col("score") <= 77.0) & (F.col("grade_status") == "INCOMPLETE"))
    .select(
        "id",
        "student_id",
        "course_id",
        "term",
        "grade_status",
        "enrolled_at",
        F.coalesce(F.col("score"), F.lit(0.0)).alias("score_filled"),
    )
)

with_windows = (
    recent_enrollments
    .select(
        "*",
        F.round(F.col("score_filled") - F.avg("score_filled").over(w_student_id_term), 2).alias("score_filled_vs_student_id_term_avg"),
        F.first("score_filled").over(w_all_by_enrolled_at_full).alias("first_score_filled"),
        F.sum("score_filled").over(w_enrolled_at_month_course_id).alias("enrolled_at_month_course_id_sum_score_filled"),
        F.round(F.percent_rank().over(w_course_id_by_score_filled_ti), 3).alias("score_filled_percent_rank"),
        F.sum("score_filled").over(w_all_by_score_filled_running).alias("running_score_filled"),
        F.nth_value("enrolled_at", 2).over(w_term_student_id_by_score_fil).alias("second_enrolled_at"),
        F.dense_rank().over(w_all_by_score_filled).alias("score_filled_dense_rank"),
        F.size(F.collect_set("grade_status").over(w_student_id_term)).alias("distinct_grade_status_in_student_id_term"),
        F.dense_rank().over(w_student_id_course_id_by_scor).alias("score_filled_dense_rank_2"),
        F.round(F.percent_rank().over(w_student_id_course_id_by_scor), 3).alias("score_filled_percent_rank_2"),
        F.round(F.avg("score_filled").over(w_term_student_id_by_score_fil_2), 2).alias("score_filled_avg_2"),
        F.round(F.col("score_filled") / F.nullif(F.sum("score_filled").over(w_student_id_term), F.lit(0)), 4).alias("score_filled_share_of_student_id_term"),
        F.round(F.percent_rank().over(w_course_id_by_score_filled), 3).alias("score_filled_percent_rank_3"),
    )
)

scored = (
    with_windows
    .filter(F.col("score_filled_share_of_student_id_term") < 0.7)
    .select(
        "id",
        "student_id",
        "course_id",
        "term",
        "score_filled",
        "score_filled_vs_student_id_term_avg",
        "first_score_filled",
        "enrolled_at_month_course_id_sum_score_filled",
        "score_filled_percent_rank",
        "running_score_filled",
        "second_enrolled_at",
        "score_filled_dense_rank",
        "distinct_grade_status_in_student_id_term",
        "score_filled_dense_rank_2",
        "score_filled_percent_rank_2",
        "score_filled_avg_2",
        "score_filled_percent_rank_3",
        (F.floor(F.col("first_score_filled") / 25) * 25).alias("first_score_filled_bucket"),
        F.coalesce(F.col("score_filled_dense_rank"), F.lit(0)).alias("score_filled_dense_rank_filled"),
    )
)

with_trends = (
    scored
    .select(
        "*",
        F.ntile(4).over(w_score_filled_dense_rank_by_r).alias("running_score_filled_quartile"),
        F.round(F.avg("score_filled_avg_2").over(w_term_student_id), 2).alias("term_student_id_avg_score_filled_avg_2"),
        F.sum("first_score_filled_bucket").over(w_score_filled_dense_rank_fill).alias("running_first_score_filled_bucket"),
        (F.col("score_filled_percent_rank_2") - F.lag("score_filled_percent_rank_2").over(w_all_by_enrolled_at_month_cou)).alias("score_filled_percent_rank_2_change"),
        (F.col("score_filled_percent_rank") - F.lag("score_filled_percent_rank").over(w_score_filled_dense_rank_by_r)).alias("score_filled_percent_rank_change"),
        F.lead("score_filled", 2, 0).over(w_course_id_by_enrolled_at_mon).alias("next2_score_filled"),
    )
)

result = (
    with_trends
    .select("score_filled_dense_rank_2", "score_filled_percent_rank_2", "id")
    .distinct()
    .orderBy("score_filled_percent_rank_2", "score_filled_dense_rank_2")
)
