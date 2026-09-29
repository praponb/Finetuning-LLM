from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_grade_status_course_level_by = Window.partitionBy("grade_status", "course_level").orderBy(F.col("student_gpa").asc_nulls_last(), "id")
w_grade_status_course_level_by_2 = Window.partitionBy("grade_status", "course_level").orderBy(F.col("student_gpa").asc_nulls_last(), "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_all_by_enrolled_at_moving_6 = Window.orderBy("enrolled_at", "id").rowsBetween(-6, Window.currentRow)
w_all_by_student_gpa = Window.orderBy(F.col("student_gpa").desc_nulls_first(), "id")
w_all_by_student_gpa_prior_1 = Window.orderBy(F.col("student_gpa").desc_nulls_first(), "id").rowsBetween(-1, -1)
w_course_level_term_by_enrolle = Window.partitionBy("course_level", "term").orderBy("enrolled_at")
w_rn_2_grade_status = Window.partitionBy("rn_2", "grade_status")
w_all_by_student_gpa_dense_ran = Window.orderBy("student_gpa_dense_rank", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_all_by_student_gpa_dense_ran_2 = Window.orderBy("student_gpa_dense_rank", "id")
w_course_level = Window.partitionBy("course_level")
w_course_level_by_student_gpa = Window.partitionBy("course_level").orderBy("student_gpa_dense_rank", "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)

recent = (
    df_enrollments
    .filter(F.col("student_id") >= 16)
    .select(
        "id",
        "student_id",
        "course_id",
        "term",
        "score",
        "grade_status",
        "enrolled_at",
    )
)

with_details_course = (
    recent.alias("s")
    .join(df_courses.alias("c"), F.col("c.id") == F.col("s.course_id"), "left")
    .join(df_students.alias("st"), F.col("st.id") == F.col("s.student_id"), "left")
    .select(
        "s.*",
        F.col("c.level").alias("course_level"),
        F.col("st.gpa").alias("student_gpa"),
        "st.student_name",
    )
)

metrics = (
    with_details_course
    .select(
        "*",
        F.lead("enrolled_at").over(w_grade_status_course_level_by).alias("next_enrolled_at"),
        F.nth_value("score", 2).over(w_grade_status_course_level_by_2).alias("second_score"),
        F.round(F.cume_dist().over(w_grade_status_course_level_by), 3).alias("student_gpa_cume_dist"),
        F.max("student_gpa").over(w_all_by_enrolled_at_moving_6).alias("student_gpa_max_7"),
        F.row_number().over(w_all_by_student_gpa).alias("rn"),
        F.row_number().over(w_grade_status_course_level_by).alias("rn_2"),
        F.round(F.avg("score").over(w_all_by_student_gpa_prior_1), 2).alias("score_avg_1_prior"),
        F.dense_rank().over(w_all_by_student_gpa).alias("student_gpa_dense_rank"),
        F.dense_rank().over(w_grade_status_course_level_by).alias("student_gpa_dense_rank_2"),
        F.round(F.percent_rank().over(w_course_level_term_by_enrolle), 3).alias("enrolled_at_percent_rank"),
    )
)

with_trends = (
    metrics
    .select(
        "*",
        F.size(F.collect_set("term").over(w_rn_2_grade_status)).alias("distinct_term_in_rn_2_grade_status"),
        F.min("enrolled_at_percent_rank").over(w_all_by_student_gpa_dense_ran).alias("min_enrolled_at_percent_rank_so_far"),
        F.ntile(3).over(w_all_by_student_gpa_dense_ran_2).alias("student_gpa_dense_rank_tercile"),
        F.count(F.lit(1)).over(w_course_level).alias("rows_in_course_level"),
        F.last("enrolled_at_percent_rank").over(w_course_level_by_student_gpa).alias("last_enrolled_at_percent_rank"),
        F.round(F.col("enrolled_at_percent_rank") - F.avg("enrolled_at_percent_rank").over(w_course_level), 2).alias("enrolled_at_percent_rank_vs_course_level_avg"),
        F.count(F.lit(1)).over(w_rn_2_grade_status).alias("rows_in_rn_2_grade_status"),
        F.round(F.col("rn") / F.nullif(F.sum("rn").over(w_rn_2_grade_status), F.lit(0)), 4).alias("rn_share_of_rn_2_grade_status"),
    )
)

result = (
    with_trends
    .filter(F.hour(F.col("next_enrolled_at")).between(10, 16))
    .select(
        "id",
        "course_id",
        "term",
        "score",
        "grade_status",
        "enrolled_at",
        "course_level",
        "student_name",
        "next_enrolled_at",
        "second_score",
        "student_gpa_cume_dist",
        "student_gpa_max_7",
        "rn",
        "student_gpa_dense_rank_2",
        "min_enrolled_at_percent_rank_so_far",
        "student_gpa_dense_rank_tercile",
        "rows_in_course_level",
        "last_enrolled_at_percent_rank",
        "enrolled_at_percent_rank_vs_course_level_avg",
        "rows_in_rn_2_grade_status",
        "rn_share_of_rn_2_grade_status",
    )
)
