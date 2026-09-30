from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_enrolled_at_year_student_is = Window.partitionBy(F.year("enrolled_at"), "student_is_international")
w_course_credits_by_course_cre = Window.partitionBy("course_credits").orderBy(F.col("course_credits").desc_nulls_first(), "id")
w_grade_status_course_id_by_sc = Window.partitionBy("grade_status", "course_id").orderBy(F.col("score").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_grade_status_or_unknown_by_e = Window.partitionBy("grade_status_or_unknown").orderBy(F.col("enrolled_at").desc(), "id").rowsBetween(-6, -1)
w_grade_status_student_major_b = Window.partitionBy("grade_status", "student_major").orderBy("score", "id")
w_course_credits_by_score_full = Window.partitionBy("course_credits").orderBy(F.col("score").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_course_credits_by_course_cre_2 = Window.partitionBy("course_credits").orderBy(F.col("course_credits").desc_nulls_first(), "id").rowsBetween(-3, Window.currentRow)
w_all_by_score_sum_6_prior = Window.orderBy(F.col("score_sum_6_prior").desc(), "id")
w_all_by_score_sum_6_prior_tie = Window.orderBy(F.col("score_sum_6_prior").desc())
w_student_is_international = Window.partitionBy("student_is_international")
w_all_by_score_sum_6_prior_ful = Window.orderBy(F.col("score_sum_6_prior").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)

valid_enrollments = (
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
        F.coalesce(F.col("grade_status"), F.lit("UNKNOWN")).alias("grade_status_or_unknown"),
    )
)

with_details_course = (
    valid_enrollments.alias("s")
    .join(df_courses.alias("c"), F.col("c.id") == F.col("s.course_id"), "left")
    .join(df_students.alias("st"), F.col("st.id") == F.col("s.student_id"), "left")
    .select(
        "s.*",
        F.col("c.credits").alias("course_credits"),
        F.col("st.is_international").alias("student_is_international"),
        F.col("st.major").alias("student_major"),
    )
)

windowed = (
    with_details_course
    .select(
        "*",
        F.count(F.lit(1)).over(w_enrolled_at_year_student_is).alias("rows_in_enrolled_at_year_student_is_internationa"),
        F.lead("enrolled_at", 2).over(w_course_credits_by_course_cre).alias("next2_enrolled_at"),
        F.lag("score", 3).over(w_course_credits_by_course_cre).alias("prev3_score"),
        F.nth_value("enrolled_at", 2).over(w_grade_status_course_id_by_sc).alias("second_enrolled_at"),
        F.sum("score").over(w_grade_status_or_unknown_by_e).alias("score_sum_6_prior"),
        F.round(F.percent_rank().over(w_course_credits_by_course_cre), 3).alias("course_credits_percent_rank"),
        F.lead("enrolled_at", 2).over(w_grade_status_student_major_b).alias("next2_enrolled_at_2"),
        F.nth_value("enrolled_at", 2).over(w_course_credits_by_score_full).alias("second_enrolled_at_2"),
        F.round(F.avg("score").over(w_course_credits_by_course_cre_2), 2).alias("score_avg_4"),
    )
)

scored = (
    windowed
    .filter(F.col("course_credits") < 4)
    .select(
        "*",
        F.greatest(F.col("course_credits"), F.col("score_sum_6_prior")).alias("greatest_course_credits_score_sum_6_prior"),
        F.coalesce(F.col("score_avg_4"), F.lit(0.0)).alias("score_avg_4_filled"),
    )
)

with_trends = (
    scored
    .select(
        "id",
        "student_id",
        "course_id",
        "term",
        "score",
        "enrolled_at",
        "grade_status_or_unknown",
        "student_is_international",
        "rows_in_enrolled_at_year_student_is_internationa",
        "next2_enrolled_at",
        "prev3_score",
        "second_enrolled_at",
        "score_sum_6_prior",
        "course_credits_percent_rank",
        "next2_enrolled_at_2",
        "second_enrolled_at_2",
        "score_avg_4",
        "score_avg_4_filled",
        F.row_number().over(w_all_by_score_sum_6_prior).alias("seq_no"),
        F.round(F.percent_rank().over(w_all_by_score_sum_6_prior_tie), 3).alias("score_sum_6_prior_percent_rank"),
        F.round(F.avg("course_credits_percent_rank").over(w_student_is_international), 2).alias("student_is_international_avg_course_credits_perc"),
        F.nth_value("second_enrolled_at", 2).over(w_all_by_score_sum_6_prior_ful).alias("second_second_enrolled_at"),
    )
)

result = (
    with_trends
    .select(
        "id",
        "student_id",
        "course_id",
        "term",
        "score",
        "enrolled_at",
        "grade_status_or_unknown",
        "rows_in_enrolled_at_year_student_is_internationa",
        "next2_enrolled_at",
        "prev3_score",
        "second_enrolled_at",
        "score_sum_6_prior",
        "course_credits_percent_rank",
        "second_enrolled_at_2",
        "score_avg_4",
        "score_avg_4_filled",
        "seq_no",
        "student_is_international_avg_course_credits_perc",
        "second_second_enrolled_at",
        F.lower(F.regexp_replace(F.trim(F.col("grade_status_or_unknown")), " ", "_")).alias("grade_status_or_unknown_slug"),
        F.abs(F.col("score") - 35.8).alias("score_distance"),
    )
    .orderBy("student_is_international_avg_course_credits_perc")
)
