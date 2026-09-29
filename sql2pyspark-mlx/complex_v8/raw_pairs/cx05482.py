from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_term = Window.partitionBy("term")
w_course_id_student_id = Window.partitionBy("course_id", "student_id")
w_grade_status_term = Window.partitionBy("grade_status", "term")
w_all_by_score_adj = Window.orderBy("score_adj", "id")
w_grade_status_term_by_score_d = Window.partitionBy("grade_status", "term").orderBy(F.col("score_distance").desc(), "id").rowsBetween(-3, -1)
w_grade_status_enrolled_at_mon = Window.partitionBy("grade_status", F.date_trunc("month", "enrolled_at")).orderBy(F.col("score_distance").asc_nulls_last(), "id").rowsBetween(-3, -1)
w_grade_status_term_by_score_d_2 = Window.partitionBy("grade_status", "term").orderBy(F.col("score_distance").desc(), "id")
w_grade_status_enrolled_at_mon_2 = Window.partitionBy("grade_status", F.date_trunc("month", "enrolled_at")).orderBy(F.col("score_distance").asc_nulls_last(), "id")
w_enrolled_at_month_by_enrolle = Window.partitionBy(F.date_trunc("month", "enrolled_at")).orderBy(F.col("enrolled_at").desc())
w_grade_status_term_by_score_d_3 = Window.partitionBy("grade_status", "term").orderBy(F.col("score_distance").desc())
w_student_id_by_enrolled_at_ti = Window.partitionBy("student_id").orderBy(F.col("enrolled_at").asc_nulls_last())

cleaned_enrollments = (
    df_enrollments
    .filter(F.col("enrolled_at") >= F.lit("2025-01-31 08:00:00").cast("timestamp"))
    .select(
        "id",
        "student_id",
        "course_id",
        "term",
        "grade_status",
        "enrolled_at",
        F.round(F.col("score") * 0.25, 2).alias("score_adj"),
        F.abs(F.col("score") - 57.6).alias("score_distance"),
    )
)

windowed = (
    cleaned_enrollments
    .select(
        "id",
        "enrolled_at",
        "score_adj",
        "score_distance",
        F.round(F.avg("score_adj").over(w_term), 2).alias("term_avg_score_adj"),
        F.sum("score_adj").over(w_term).alias("term_sum_score_adj"),
        F.max("score_adj").over(w_course_id_student_id).alias("course_id_student_id_max_score_adj"),
        F.round(F.col("score_distance") / F.nullif(F.sum("score_distance").over(w_grade_status_term), F.lit(0)), 4).alias("score_distance_share_of_grade_status_term"),
        F.rank().over(w_all_by_score_adj).alias("score_adj_rank"),
        F.max("score_distance").over(w_grade_status_term_by_score_d).alias("score_distance_max_3_prior"),
        F.round(F.avg("score_adj").over(w_grade_status_enrolled_at_mon), 2).alias("score_adj_avg_3_prior"),
        F.max("score_distance").over(w_course_id_student_id).alias("course_id_student_id_max_score_distance"),
        F.lead("score_distance", 1, 0).over(w_grade_status_term_by_score_d_2).alias("next_score_distance"),
        F.lag("score_adj").over(w_grade_status_enrolled_at_mon_2).alias("prev_score_adj"),
        F.round(F.percent_rank().over(w_enrolled_at_month_by_enrolle), 3).alias("enrolled_at_percent_rank"),
        F.round(F.cume_dist().over(w_grade_status_term_by_score_d_3), 3).alias("score_distance_cume_dist"),
        F.rank().over(w_student_id_by_enrolled_at_ti).alias("enrolled_at_rank"),
    )
)

result = (
    windowed
    .select(
        "id",
        "enrolled_at",
        "score_adj",
        "score_distance",
        "term_avg_score_adj",
        "term_sum_score_adj",
        "score_distance_share_of_grade_status_term",
        "score_adj_rank",
        "score_distance_max_3_prior",
        "score_adj_avg_3_prior",
        "course_id_student_id_max_score_distance",
        "next_score_distance",
        "prev_score_adj",
        "enrolled_at_percent_rank",
        "score_distance_cume_dist",
        "enrolled_at_rank",
    )
    .orderBy("score_adj_avg_3_prior")
)
