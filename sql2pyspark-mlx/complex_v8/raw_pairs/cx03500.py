from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_student_id_by_course_credits = Window.partitionBy("student_id").orderBy("course_credits", "id")
w_course_credits_student_enrol = Window.partitionBy("course_credits", F.date_trunc("month", "student_enrolled_on")).orderBy(F.col("enrolled_at_year").desc(), "id")
w_course_credits_course_id = Window.partitionBy("course_credits", "course_id")
w_course_credits_student_enrol_2 = Window.partitionBy("course_credits", F.date_trunc("month", "student_enrolled_on")).orderBy(F.col("enrolled_at_year").desc(), "id").rowsBetween(-2, Window.currentRow)
w_student_id_by_course_credits_2 = Window.partitionBy("student_id").orderBy("course_credits", "id").rowsBetween(-6, -1)
w_prev_course_credits_next3_co = Window.partitionBy("prev_course_credits", "next3_course_credits_filled")
w_prev_student_enrolled_on_yea = Window.partitionBy(F.year("prev_student_enrolled_on"), "prev_course_credits").orderBy("score_avg_6_prior", "id")
w_prev_student_enrolled_on_mon = Window.partitionBy(F.month("prev_student_enrolled_on"))

filtered_enrollments = (
    df_enrollments
    .filter(F.col("enrolled_at") >= F.lit("2025-01-13 11:00:00").cast("timestamp"))
    .select(
        "id",
        "student_id",
        "course_id",
        "term",
        "score",
        "grade_status",
        "enrolled_at",
        F.date_trunc("year", F.col("enrolled_at")).alias("enrolled_at_year"),
        F.concat_ws("|", F.col("term"), F.col("grade_status")).alias("term_grade_status_label"),
    )
)

with_details_student = (
    filtered_enrollments.alias("s")
    .join(df_students.alias("st"), (F.col("st.id") == F.col("s.student_id")) & (F.upper(F.trim(F.col("st.student_name"))).like("L%")), "left")
    .join(df_courses.alias("c"), F.col("c.id") == F.col("s.course_id"), "left")
    .select(
        "s.id",
        "s.student_id",
        "s.course_id",
        "s.score",
        "s.grade_status",
        "s.enrolled_at_year",
        F.col("st.enrolled_on").alias("student_enrolled_on"),
        F.col("st.cohort").alias("student_cohort"),
        F.col("st.major").alias("student_major"),
        F.col("c.credits").alias("course_credits"),
    )
)

sequenced = (
    with_details_student
    .select(
        "*",
        F.lag("student_enrolled_on").over(w_student_id_by_course_credits).alias("prev_student_enrolled_on"),
        F.lead("course_credits", 3).over(w_course_credits_student_enrol).alias("next3_course_credits"),
        F.count(F.lit(1)).over(w_course_credits_course_id).alias("rows_in_course_credits_course_id"),
        F.min("score").over(w_course_credits_course_id).alias("course_credits_course_id_min_score"),
        F.sum("score").over(w_course_credits_course_id).alias("course_credits_course_id_sum_score"),
        F.round(F.avg("score").over(w_course_credits_student_enrol_2), 2).alias("score_avg_3"),
        F.lag("course_credits", 1, 0).over(w_student_id_by_course_credits).alias("prev_course_credits"),
        F.round(F.avg("score").over(w_student_id_by_course_credits_2), 2).alias("score_avg_6_prior"),
    )
)

labeled = (
    sequenced
    .select(
        "id",
        "course_id",
        "grade_status",
        "enrolled_at_year",
        "student_enrolled_on",
        "student_major",
        "prev_student_enrolled_on",
        "course_credits_course_id_min_score",
        "course_credits_course_id_sum_score",
        "prev_course_credits",
        "score_avg_6_prior",
        F.to_date(F.col("enrolled_at_year")).alias("enrolled_at_year_date"),
        (F.floor(F.col("score_avg_3") / 25) * 25).alias("score_avg_3_bucket"),
        F.coalesce(F.col("next3_course_credits"), F.lit(0)).alias("next3_course_credits_filled"),
    )
)

with_windows = (
    labeled
    .select(
        "*",
        F.count(F.lit(1)).over(w_prev_course_credits_next3_co).alias("rows_in_prev_course_credits_next3_course_credits"),
        (F.col("score_avg_3_bucket") - F.lag("score_avg_3_bucket").over(w_prev_student_enrolled_on_yea)).alias("score_avg_3_bucket_change"),
        F.round(F.avg("score_avg_6_prior").over(w_prev_student_enrolled_on_mon), 2).alias("prev_student_enrolled_on_month_avg_score_avg_6_p"),
        F.round(F.col("next3_course_credits_filled") / F.nullif(F.sum("next3_course_credits_filled").over(w_prev_course_credits_next3_co), F.lit(0)), 4).alias("next3_course_credits_filled_share_of_prev_course"),
        F.count(F.lit(1)).over(w_prev_student_enrolled_on_mon).alias("rows_in_prev_student_enrolled_on_month"),
    )
)

result = (
    with_windows
    .filter(F.col("score_avg_6_prior") <= 60.0)
    .select(
        "id",
        "course_id",
        "grade_status",
        "student_enrolled_on",
        "student_major",
        "prev_student_enrolled_on",
        "course_credits_course_id_min_score",
        "course_credits_course_id_sum_score",
        "prev_course_credits",
        "enrolled_at_year_date",
        "score_avg_3_bucket",
        "next3_course_credits_filled",
        "rows_in_prev_course_credits_next3_course_credits",
        "score_avg_3_bucket_change",
        "prev_student_enrolled_on_month_avg_score_avg_6_p",
        "next3_course_credits_filled_share_of_prev_course",
        "rows_in_prev_student_enrolled_on_month",
    )
    .orderBy(F.col("score_avg_3_bucket").desc())
)
