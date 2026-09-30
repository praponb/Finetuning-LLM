from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_course_id_by_enrolled_at_ful = Window.partitionBy("course_id").orderBy(F.col("enrolled_at").asc_nulls_last(), "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_all_by_enrolled_at_moving_6 = Window.orderBy("enrolled_at", "id").rowsBetween(-6, Window.currentRow)
w_course_id_by_enrolled_at = Window.partitionBy("course_id").orderBy(F.col("enrolled_at").asc_nulls_last(), "id")
w_course_id_by_enrolled_at_tie = Window.partitionBy("course_id").orderBy(F.col("enrolled_at").asc_nulls_last())
w_enrolled_at_month_grade_stat = Window.partitionBy(F.date_trunc("month", "enrolled_at"), "grade_status")
w_all_by_enrolled_at = Window.orderBy("enrolled_at", "id")
w_course_id_by_enrolled_at_run = Window.partitionBy("course_id").orderBy(F.col("enrolled_at").asc_nulls_last(), "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_course_id_by_enrolled_at_pri = Window.partitionBy("course_id").orderBy(F.col("enrolled_at").asc_nulls_last(), "id").rowsBetween(-6, -1)
w_grade_status_student_id_by_s = Window.partitionBy("grade_status", "student_id").orderBy(F.col("score_avg_6_prior").desc(), "id")
w_distinct_term_in_enrolled_at = Window.partitionBy("distinct_term_in_enrolled_at_month_grade_status", "student_id")

cleaned_enrollments = (
    df_enrollments
    .filter((F.col("grade_status") == "FAIL") | (F.col("enrolled_at") < F.lit("2025-02-18 11:00:00").cast("timestamp")))
    .select(
        "id",
        "student_id",
        "course_id",
        "term",
        "score",
        "grade_status",
        "enrolled_at",
        F.lower(F.regexp_replace(F.trim(F.col("term")), " ", "_")).alias("term_slug"),
    )
)

with_details_course = (
    cleaned_enrollments.alias("s")
    .join(df_courses.alias("c"), F.col("c.id") == F.col("s.course_id"), "inner")
    .select(
        "s.id",
        "s.student_id",
        "s.course_id",
        "s.term",
        "s.score",
        "s.grade_status",
        "s.enrolled_at",
        "c.course_code",
        F.col("c.level").alias("course_level"),
    )
)

windowed = (
    with_details_course
    .select(
        "*",
        F.nth_value("enrolled_at", 2).over(w_course_id_by_enrolled_at_ful).alias("second_enrolled_at"),
        F.round(F.avg("score").over(w_all_by_enrolled_at_moving_6), 2).alias("score_avg_7"),
        F.lag("score", 2).over(w_course_id_by_enrolled_at).alias("prev2_score"),
        F.round(F.cume_dist().over(w_course_id_by_enrolled_at_tie), 3).alias("enrolled_at_cume_dist"),
        F.round(F.col("score") - F.avg("score").over(w_enrolled_at_month_grade_stat), 2).alias("score_vs_enrolled_at_month_grade_status_avg"),
        F.count(F.lit(1)).over(w_enrolled_at_month_grade_stat).alias("rows_in_enrolled_at_month_grade_status"),
        F.size(F.collect_set("student_id").over(w_enrolled_at_month_grade_stat)).alias("distinct_student_id_in_enrolled_at_month_grade_s"),
        F.size(F.collect_set("term").over(w_enrolled_at_month_grade_stat)).alias("distinct_term_in_enrolled_at_month_grade_status"),
        F.lead("score").over(w_all_by_enrolled_at).alias("next_score"),
        F.max("score").over(w_course_id_by_enrolled_at_run).alias("max_score_so_far"),
        F.max("score").over(w_enrolled_at_month_grade_stat).alias("enrolled_at_month_grade_status_max_score"),
        F.round(F.cume_dist().over(w_all_by_enrolled_at), 3).alias("enrolled_at_cume_dist_2"),
        F.lead("score", 3).over(w_all_by_enrolled_at).alias("next3_score"),
        F.round(F.col("score") / F.nullif(F.sum("score").over(w_enrolled_at_month_grade_stat), F.lit(0)), 4).alias("score_share_of_enrolled_at_month_grade_status"),
        F.round(F.avg("score").over(w_course_id_by_enrolled_at_pri), 2).alias("score_avg_6_prior"),
    )
)

with_trends = (
    windowed
    .select(
        "*",
        F.round(F.cume_dist().over(w_grade_status_student_id_by_s), 3).alias("score_avg_6_prior_cume_dist"),
        F.round(F.col("max_score_so_far") / F.nullif(F.sum("max_score_so_far").over(w_distinct_term_in_enrolled_at), F.lit(0)), 4).alias("max_score_so_far_share_of_distinct_term_in_enrol"),
        F.round((F.unix_timestamp("enrolled_at") - F.unix_timestamp(F.lag("enrolled_at").over(w_grade_status_student_id_by_s))) / 60, 1).alias("minutes_since_prev_enrolled_at"),
    )
)

result = (
    with_trends
    .filter(F.col("next_score") < 57.6)
    .select(
        "id",
        "student_id",
        "course_id",
        "term",
        "score",
        "course_code",
        "second_enrolled_at",
        "score_avg_7",
        "prev2_score",
        "rows_in_enrolled_at_month_grade_status",
        "next_score",
        "max_score_so_far",
        "enrolled_at_month_grade_status_max_score",
        "next3_score",
        "score_share_of_enrolled_at_month_grade_status",
        "score_avg_6_prior",
        "score_avg_6_prior_cume_dist",
        "minutes_since_prev_enrolled_at",
        F.lower(F.regexp_replace(F.trim(F.col("term")), " ", "_")).alias("term_slug"),
    )
    .orderBy(F.col("id").asc_nulls_last())
)
