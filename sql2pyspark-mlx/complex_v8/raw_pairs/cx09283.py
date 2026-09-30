from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_all_by_enrolled_at = Window.orderBy("enrolled_at", "id")
w_term_course_id_by_enrolled_a = Window.partitionBy("term", "course_id").orderBy("enrolled_at", "id")
w_all_by_student_gpa_full = Window.orderBy("student_gpa", "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_all_by_student_gpa = Window.orderBy("student_gpa", "id")
w_all_by_enrolled_at_full = Window.orderBy("enrolled_at", "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_all_by_student_gpa_ties = Window.orderBy("student_gpa")
w_all_by_student_gpa_2 = Window.orderBy(F.col("student_gpa").desc_nulls_first(), "id")
w_term_course_id_by_enrolled_a_2 = Window.partitionBy("term", "course_id").orderBy("enrolled_at", "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_course_id_student_id = Window.partitionBy("course_id", "student_id")
w_all_by_enrolled_at_prior_3 = Window.orderBy("enrolled_at", "id").rowsBetween(-3, -1)
w_course_id = Window.partitionBy("course_id")
w_seq_no_by_second_enrolled_at = Window.partitionBy("seq_no").orderBy(F.col("second_enrolled_at_2").desc())
w_student_cohort_student_id_by = Window.partitionBy("student_cohort", "student_id").orderBy("prev_enrolled_at_2", "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_second_enrolled_at_2_month_p = Window.partitionBy(F.month("second_enrolled_at_2"), F.year("prev_enrolled_at")).orderBy("enrolled_at", "id").rowsBetween(-3, -1)
w_student_cohort_student_id_by_2 = Window.partitionBy("student_cohort", "student_id").orderBy("prev_enrolled_at_2", "id")
w_seq_no_by_second_enrolled_at_2 = Window.partitionBy("seq_no").orderBy(F.col("second_enrolled_at_2").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_second_enrolled_at_2_month_p_2 = Window.partitionBy(F.month("second_enrolled_at_2"), F.year("prev_enrolled_at")).orderBy("enrolled_at")
w_second_enrolled_at_month_stu = Window.partitionBy(F.month("second_enrolled_at"), "student_cohort")

cleaned_enrollments = (
    df_enrollments
    .filter(F.to_date(F.col("enrolled_at")) < F.lit("2025-02-06").cast("date"))
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

enriched_student = (
    cleaned_enrollments.alias("s")
    .join(df_students.alias("st"), (F.col("st.id") == F.col("s.student_id")) & (F.col("st.major").isNotNull()), "inner")
    .select(
        "s.id",
        "s.student_id",
        "s.course_id",
        "s.term",
        "s.grade_status",
        "s.enrolled_at",
        F.col("st.cohort").alias("student_cohort"),
        F.col("st.gpa").alias("student_gpa"),
    )
)

with_trends = (
    enriched_student
    .select(
        "*",
        F.lag("enrolled_at").over(w_all_by_enrolled_at).alias("prev_enrolled_at"),
        F.round(F.cume_dist().over(w_term_course_id_by_enrolled_a), 3).alias("enrolled_at_cume_dist"),
        F.nth_value("enrolled_at", 2).over(w_all_by_student_gpa_full).alias("second_enrolled_at"),
        F.row_number().over(w_all_by_student_gpa).alias("seq_no"),
        F.nth_value("enrolled_at", 2).over(w_all_by_enrolled_at_full).alias("second_enrolled_at_2"),
        F.dense_rank().over(w_all_by_student_gpa_ties).alias("student_gpa_dense_rank"),
        (F.col("student_gpa") - F.lag("student_gpa").over(w_all_by_student_gpa_2)).alias("student_gpa_change"),
        F.nth_value("enrolled_at", 2).over(w_term_course_id_by_enrolled_a_2).alias("second_enrolled_at_3"),
        F.max("student_gpa").over(w_course_id_student_id).alias("course_id_student_id_max_student_gpa"),
        F.sum("student_gpa").over(w_all_by_enrolled_at_prior_3).alias("student_gpa_sum_3_prior"),
        F.max("student_gpa").over(w_course_id).alias("course_id_max_student_gpa"),
        F.lag("enrolled_at").over(w_term_course_id_by_enrolled_a).alias("prev_enrolled_at_2"),
        F.size(F.collect_set("grade_status").over(w_course_id)).alias("distinct_grade_status_in_course_id"),
    )
)

sequenced = (
    with_trends
    .select(
        "id",
        "student_id",
        "course_id",
        "term",
        "enrolled_at",
        "student_gpa",
        "prev_enrolled_at",
        "enrolled_at_cume_dist",
        "student_gpa_dense_rank",
        "second_enrolled_at_3",
        "course_id_student_id_max_student_gpa",
        "course_id_max_student_gpa",
        "prev_enrolled_at_2",
        "distinct_grade_status_in_course_id",
        F.round(F.percent_rank().over(w_seq_no_by_second_enrolled_at), 3).alias("second_enrolled_at_2_percent_rank"),
        F.first("prev_enrolled_at_2").over(w_student_cohort_student_id_by).alias("first_prev_enrolled_at_2"),
        F.round(F.avg("student_gpa_change").over(w_second_enrolled_at_2_month_p), 2).alias("student_gpa_change_avg_3_prior"),
        F.ntile(5).over(w_student_cohort_student_id_by_2).alias("prev_enrolled_at_2_quintile"),
        F.last("second_enrolled_at", ignorenulls=True).over(w_seq_no_by_second_enrolled_at_2).alias("last_known_second_enrolled_at"),
        F.round(F.percent_rank().over(w_second_enrolled_at_2_month_p_2), 3).alias("enrolled_at_percent_rank"),
        F.min("enrolled_at_cume_dist").over(w_second_enrolled_at_month_stu).alias("second_enrolled_at_month_student_cohort_min_enro"),
    )
)

stats = (
    sequenced
    .groupBy(
        F.year("first_prev_enrolled_at_2").alias("first_prev_enrolled_at_2_year"),
        F.date_trunc("month", "second_enrolled_at_3").alias("second_enrolled_at_3_month"),
        "course_id",
    )
    .agg(
        F.min("prev_enrolled_at").alias("first_prev_enrolled_at"),
        F.count("*").alias("cnt"),
        F.round(F.sum("course_id_student_id_max_student_gpa") / F.nullif(F.sum("student_gpa_dense_rank"), F.lit(0)), 3).alias("course_id_student_id_max_student_gpa_to_student_"),
    )
    .filter(F.col("cnt") >= 2)
)

result = (
    stats
    .select("course_id")
    .distinct()
    .orderBy(F.col("course_id").desc())
)
