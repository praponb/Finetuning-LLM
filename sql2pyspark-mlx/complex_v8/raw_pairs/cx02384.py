from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_grade_status_student_id_by_e = Window.partitionBy("grade_status", "student_id").orderBy("enrolled_at_week")
w_all_by_enrolled_at_week = Window.orderBy(F.col("enrolled_at_week").desc(), "id")
w_course_id_student_id = Window.partitionBy("course_id", "student_id")
w_grade_status_student_id_by_e_2 = Window.partitionBy("grade_status", "student_id").orderBy("enrolled_at_week", "id")
w_all_by_enrolled_at_week_runn = Window.orderBy("enrolled_at_week", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_student_id = Window.partitionBy("student_id")

filtered = (
    df_enrollments
    .filter(F.col("course_id") <= 3)
    .select(
        "id",
        "student_id",
        "course_id",
        "term",
        "score",
        "grade_status",
        "enrolled_at",
        F.upper(F.trim(F.col("grade_status"))).alias("grade_status_clean"),
        F.date_trunc("week", F.col("enrolled_at")).alias("enrolled_at_week"),
    )
)

windowed = (
    filtered
    .select(
        "*",
        F.dense_rank().over(w_grade_status_student_id_by_e).alias("enrolled_at_week_dense_rank"),
        F.lag("enrolled_at", 2).over(w_all_by_enrolled_at_week).alias("prev2_enrolled_at"),
        F.max("score").over(w_course_id_student_id).alias("course_id_student_id_max_score"),
        F.ntile(4).over(w_grade_status_student_id_by_e_2).alias("enrolled_at_week_quartile"),
        F.round(F.col("score") - F.avg("score").over(w_course_id_student_id), 2).alias("score_vs_course_id_student_id_avg"),
        F.round(F.percent_rank().over(w_grade_status_student_id_by_e), 3).alias("enrolled_at_week_percent_rank"),
        F.min("score").over(w_all_by_enrolled_at_week_runn).alias("min_score_so_far"),
        F.sum("score").over(w_course_id_student_id).alias("course_id_student_id_sum_score"),
        F.count(F.lit(1)).over(w_student_id).alias("rows_in_student_id"),
        F.round((F.unix_timestamp("enrolled_at") - F.unix_timestamp(F.lag("enrolled_at").over(w_all_by_enrolled_at_week))) / 60, 1).alias("minutes_since_prev_enrolled_at"),
        F.round(F.cume_dist().over(w_grade_status_student_id_by_e), 3).alias("enrolled_at_week_cume_dist"),
        F.round(F.col("score") - F.avg("score").over(w_student_id), 2).alias("score_vs_student_id_avg"),
    )
)

result = (
    windowed
    .filter(F.col("score_vs_course_id_student_id_avg") > -60.0)
    .select(
        "id",
        "student_id",
        "course_id",
        "score",
        "grade_status",
        "enrolled_at",
        "enrolled_at_week",
        "enrolled_at_week_dense_rank",
        "prev2_enrolled_at",
        "course_id_student_id_max_score",
        "enrolled_at_week_percent_rank",
        "min_score_so_far",
        "course_id_student_id_sum_score",
        "rows_in_student_id",
        "minutes_since_prev_enrolled_at",
        "enrolled_at_week_cume_dist",
        "score_vs_student_id_avg",
    )
    .orderBy(F.col("student_id").asc())
)
