from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_course_id_enrolled_at_to_dat = Window.partitionBy("course_id", F.to_date("enrolled_at")).orderBy("score", "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_grade_status_student_id_by_e = Window.partitionBy("grade_status", "student_id").orderBy(F.col("enrolled_at").desc(), "id")
w_student_id_grade_status = Window.partitionBy("student_id", "grade_status")
w_student_id_by_score = Window.partitionBy("student_id").orderBy("score", "id")
w_grade_status_student_id_by_e_2 = Window.partitionBy("grade_status", "student_id").orderBy(F.col("enrolled_at").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_grade_status_student_id_by_e_3 = Window.partitionBy("grade_status", "student_id").orderBy(F.col("enrolled_at").desc())
w_course_id_enrolled_at_to_dat_2 = Window.partitionBy("course_id", F.to_date("enrolled_at")).orderBy("score", "id")
w_score_rank_student_id = Window.partitionBy("score_rank", "student_id")
w_all_by_max_score_so_far = Window.orderBy(F.col("max_score_so_far").desc(), "id")
w_second_enrolled_at_month_gra = Window.partitionBy(F.date_trunc("month", "second_enrolled_at"), "grade_status")
w_all_by_next2_enrolled_at = Window.orderBy(F.col("next2_enrolled_at").desc(), "id")
w_course_id_score_rank_by_next = Window.partitionBy("course_id", "score_rank").orderBy(F.col("next2_enrolled_at").desc(), "id")

base_enrollments = (
    df_enrollments
    .filter((F.col("student_id").isNotNull()) & (F.col("course_id").isNotNull()))
    .select(
        "id",
        "student_id",
        "course_id",
        "term",
        "score",
        "grade_status",
        "enrolled_at",
        F.concat(F.col("term"), F.lit("#"), F.col("course_id").cast("string")).alias("term_ref"),
        F.lower(F.trim(F.col("term"))).alias("term_clean"),
    )
)

windowed = (
    base_enrollments
    .select(
        "id",
        "student_id",
        "course_id",
        "score",
        "grade_status",
        "term_clean",
        F.nth_value("enrolled_at", 2).over(w_course_id_enrolled_at_to_dat).alias("second_enrolled_at"),
        (F.col("score") - F.lag("score").over(w_grade_status_student_id_by_e)).alias("score_change"),
        F.round(F.col("score") - F.avg("score").over(w_student_id_grade_status), 2).alias("score_vs_student_id_grade_status_avg"),
        F.round(F.avg("score").over(w_student_id_grade_status), 2).alias("student_id_grade_status_avg_score"),
        F.round(F.cume_dist().over(w_student_id_by_score), 3).alias("score_cume_dist"),
        F.max("score").over(w_grade_status_student_id_by_e_2).alias("max_score_so_far"),
        F.round(F.col("score") / F.nullif(F.sum("score").over(w_student_id_grade_status), F.lit(0)), 4).alias("score_share_of_student_id_grade_status"),
        F.rank().over(w_grade_status_student_id_by_e_3).alias("enrolled_at_rank"),
        F.rank().over(w_course_id_enrolled_at_to_dat_2).alias("score_rank"),
        F.lead("enrolled_at", 2).over(w_student_id_by_score).alias("next2_enrolled_at"),
    )
)

top_rows = windowed.filter((F.col("score_rank") == 1) & (F.col("grade_status").isNotNull()))

with_windows = (
    top_rows
    .select(
        "id",
        "student_id",
        "course_id",
        "score",
        "grade_status",
        "term_clean",
        "second_enrolled_at",
        "score_vs_student_id_grade_status_avg",
        "student_id_grade_status_avg_score",
        "score_cume_dist",
        "max_score_so_far",
        "score_share_of_student_id_grade_status",
        "enrolled_at_rank",
        "score_rank",
        F.round(F.col("score") - F.avg("score").over(w_score_rank_student_id), 2).alias("score_vs_score_rank_student_id_avg"),
        F.row_number().over(w_all_by_max_score_so_far).alias("seq_no"),
        F.round(F.col("score_change") - F.avg("score_change").over(w_second_enrolled_at_month_gra), 2).alias("score_change_vs_second_enrolled_at_month_grade_s"),
        F.round(F.percent_rank().over(w_all_by_next2_enrolled_at), 3).alias("next2_enrolled_at_percent_rank"),
        F.round((F.unix_timestamp("second_enrolled_at") - F.unix_timestamp(F.lag("second_enrolled_at").over(w_course_id_score_rank_by_next))) / 60, 1).alias("minutes_since_prev_second_enrolled_at"),
    )
)

result = (
    with_windows
    .filter(F.col("course_id") <= 9)
    .select(
        "id",
        "student_id",
        "score",
        "grade_status",
        "term_clean",
        "second_enrolled_at",
        "student_id_grade_status_avg_score",
        "score_cume_dist",
        "max_score_so_far",
        "score_share_of_student_id_grade_status",
        "enrolled_at_rank",
        "score_rank",
        "score_vs_score_rank_student_id_avg",
        "seq_no",
        (F.floor(F.col("enrolled_at_rank") / 2) * 2).alias("enrolled_at_rank_bucket"),
    )
    .orderBy(F.col("enrolled_at_rank").desc())
)
