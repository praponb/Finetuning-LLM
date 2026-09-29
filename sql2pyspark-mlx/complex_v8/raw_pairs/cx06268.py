from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_all_by_course_credits = Window.orderBy("course_credits", "id")
w_course_credits_course_id_by = Window.partitionBy("course_credits", "course_id").orderBy("course_credits", "id")
w_course_id = Window.partitionBy("course_id")
w_all_by_enrolled_at_ties = Window.orderBy("enrolled_at")
w_course_credits_course_id_by_2 = Window.partitionBy("course_credits", "course_id").orderBy("course_credits", "id").rowsBetween(-3, -1)
w_all_by_enrolled_at = Window.orderBy("enrolled_at", "id")
w_course_credits_course_id_by_3 = Window.partitionBy("course_credits", "course_id").orderBy("course_credits", "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_enrolled_at_month_by_second = Window.partitionBy(F.month("enrolled_at")).orderBy("second_enrolled_at")
w_rn_second_enrolled_at_year = Window.partitionBy("rn", F.year("second_enrolled_at"))
w_course_id_enrolled_at_dense = Window.partitionBy("course_id", "enrolled_at_dense_rank")
w_prev3_enrolled_at_month_by_m = Window.partitionBy(F.month("prev3_enrolled_at")).orderBy("minutes_since_prev_enrolled_at_3", "id")
w_prev3_enrolled_at_month_by_m_2 = Window.partitionBy(F.month("prev3_enrolled_at")).orderBy("minutes_since_prev_enrolled_at_3", "id").rowsBetween(-6, -1)
w_prev3_enrolled_at_month_by_m_3 = Window.partitionBy(F.month("prev3_enrolled_at")).orderBy("minutes_since_prev_enrolled_at_3", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

base_enrollments = (
    df_enrollments
    .filter((F.col("enrolled_at") < F.lit("2025-02-18 10:00:00").cast("timestamp")) & (F.col("course_id").isNotNull()))
    .select(
        "id",
        "student_id",
        "course_id",
        "score",
        "grade_status",
        "enrolled_at",
        F.concat(F.col("grade_status"), F.lit("#"), F.col("student_id").cast("string")).alias("grade_status_ref"),
    )
)

combined_details_student = (
    base_enrollments.alias("s")
    .join(df_students.alias("st"), F.col("st.id") == F.col("s.student_id"), "left")
    .join(df_courses.alias("c"), F.col("c.id") == F.col("s.course_id"), "left")
    .select(
        "s.id",
        "s.student_id",
        "s.course_id",
        "s.score",
        "s.enrolled_at",
        F.col("st.enrolled_on").alias("student_enrolled_on"),
        F.col("st.is_international").alias("student_is_international"),
        F.col("c.credits").alias("course_credits"),
    )
)

with_trends = (
    combined_details_student
    .select(
        "*",
        F.lead("score", 3).over(w_all_by_course_credits).alias("next3_score"),
        F.row_number().over(w_course_credits_course_id_by).alias("rn"),
        F.round(F.col("course_credits") / F.nullif(F.sum("course_credits").over(w_course_id), F.lit(0)), 4).alias("course_credits_share_of_course_id"),
        F.round(F.percent_rank().over(w_all_by_enrolled_at_ties), 3).alias("enrolled_at_percent_rank"),
        F.count(F.lit(1)).over(w_course_id).alias("rows_in_course_id"),
        F.round(F.avg("score").over(w_course_credits_course_id_by_2), 2).alias("score_avg_3_prior"),
        F.round((F.unix_timestamp("enrolled_at") - F.unix_timestamp(F.lag("enrolled_at").over(w_all_by_enrolled_at))) / 60, 1).alias("minutes_since_prev_enrolled_at"),
        F.nth_value("enrolled_at", 2).over(w_course_credits_course_id_by_3).alias("second_enrolled_at"),
        F.round((F.unix_timestamp("enrolled_at") - F.unix_timestamp(F.lag("enrolled_at").over(w_course_credits_course_id_by))) / 60, 1).alias("minutes_since_prev_enrolled_at_3"),
        F.first("enrolled_at").over(w_course_credits_course_id_by_3).alias("first_enrolled_at"),
        F.lag("enrolled_at", 3).over(w_course_credits_course_id_by).alias("prev3_enrolled_at"),
        F.round(F.avg("score").over(w_course_id), 2).alias("course_id_avg_score"),
        F.dense_rank().over(w_all_by_enrolled_at_ties).alias("enrolled_at_dense_rank"),
    )
)

metrics = (
    with_trends
    .select(
        "id",
        "enrolled_at",
        "student_enrolled_on",
        "course_credits",
        "next3_score",
        "course_credits_share_of_course_id",
        "enrolled_at_percent_rank",
        "rows_in_course_id",
        "score_avg_3_prior",
        "minutes_since_prev_enrolled_at",
        "second_enrolled_at",
        "minutes_since_prev_enrolled_at_3",
        "first_enrolled_at",
        "prev3_enrolled_at",
        "course_id_avg_score",
        "enrolled_at_dense_rank",
        F.round(F.cume_dist().over(w_enrolled_at_month_by_second), 3).alias("second_enrolled_at_cume_dist"),
        F.round(F.col("rows_in_course_id") / F.nullif(F.sum("rows_in_course_id").over(w_rn_second_enrolled_at_year), F.lit(0)), 4).alias("rows_in_course_id_share_of_rn_second_enrolled_at"),
        F.count(F.lit(1)).over(w_course_id_enrolled_at_dense).alias("rows_in_course_id_enrolled_at_dense_rank"),
        F.lag("next3_score", 2).over(w_prev3_enrolled_at_month_by_m).alias("prev2_next3_score"),
        F.round(F.avg("rows_in_course_id").over(w_prev3_enrolled_at_month_by_m_2), 2).alias("rows_in_course_id_avg_6_prior"),
        F.first("rows_in_course_id", ignorenulls=True).over(w_prev3_enrolled_at_month_by_m_3).alias("first_known_rows_in_course_id"),
    )
)

result = (
    metrics
    .select(
        "id",
        "enrolled_at",
        "next3_score",
        "rows_in_course_id",
        "minutes_since_prev_enrolled_at",
        "second_enrolled_at",
        "minutes_since_prev_enrolled_at_3",
        "first_enrolled_at",
        "prev3_enrolled_at",
        "enrolled_at_dense_rank",
        "rows_in_course_id_share_of_rn_second_enrolled_at",
        "rows_in_course_id_enrolled_at_dense_rank",
        "prev2_next3_score",
        "rows_in_course_id_avg_6_prior",
        F.date_trunc("month", F.col("enrolled_at")).alias("enrolled_at_month"),
        (F.col("prev2_next3_score") - F.col("course_id_avg_score")).alias("prev2_next3_score_minus_course_id_avg_score"),
    )
    .orderBy(
        F.col("next3_score").desc(),
        F.col("enrolled_at_dense_rank").asc_nulls_last(),
    )
)
