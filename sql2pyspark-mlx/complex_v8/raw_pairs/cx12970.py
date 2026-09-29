from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_all_by_score = Window.orderBy("score", "id")
w_student_major_grade_status_b = Window.partitionBy("student_major", "grade_status").orderBy(F.col("student_gpa").asc_nulls_last(), "id").rowsBetween(-2, Window.currentRow)
w_enrolled_at_month_by_student = Window.partitionBy(F.date_trunc("month", "enrolled_at")).orderBy(F.col("student_gpa").desc(), "id")
w_all_by_score_full = Window.orderBy("score", "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_student_major_grade_status_b_2 = Window.partitionBy("student_major", "grade_status").orderBy(F.col("student_gpa").asc_nulls_last(), "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_student_major_student_cohort = Window.partitionBy("student_major", "student_cohort").orderBy(F.col("enrolled_at").desc(), "id")
w_enrolled_at_to_date = Window.partitionBy(F.to_date("enrolled_at"))
w_student_id_by_enrolled_at = Window.partitionBy("student_id").orderBy(F.col("enrolled_at").desc_nulls_first(), "id")
w_student_id = Window.partitionBy("student_id")
w_all_by_score_ties = Window.orderBy("score")
w_student_major_by_score_cume = Window.partitionBy("student_major").orderBy("score_cume_dist", "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_student_major_by_score_cume_2 = Window.partitionBy("student_major").orderBy("score_cume_dist")
w_distinct_student_major_in_st = Window.partitionBy("distinct_student_major_in_student_id")
w_all_by_first_score = Window.orderBy(F.col("first_score").desc(), "id")
w_student_major_by_score_cume_3 = Window.partitionBy("student_major").orderBy("score_cume_dist", "id")

base = (
    df_enrollments
    .filter((F.col("student_id").isNotNull()) & (F.col("enrolled_at") >= F.lit("2025-02-06 11:00:00").cast("timestamp")))
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

joined_student = (
    base.alias("s")
    .join(df_students.alias("st"), F.col("st.id") == F.col("s.student_id"), "inner")
    .join(df_courses.alias("c"), F.col("c.id") == F.col("s.course_id"), "inner")
    .select(
        "s.id",
        "s.student_id",
        "s.course_id",
        "s.score",
        "s.grade_status",
        "s.enrolled_at",
        F.col("st.major").alias("student_major"),
        F.col("st.cohort").alias("student_cohort"),
        F.col("st.gpa").alias("student_gpa"),
        "c.course_code",
    )
)

windowed = (
    joined_student
    .select(
        "*",
        F.row_number().over(w_all_by_score).alias("seq_no"),
        F.sum("score").over(w_student_major_grade_status_b).alias("score_sum_3"),
        (F.col("student_gpa") - F.lag("student_gpa").over(w_enrolled_at_month_by_student)).alias("student_gpa_change"),
        F.round(F.percent_rank().over(w_all_by_score), 3).alias("score_percent_rank"),
        F.first("score").over(w_all_by_score_full).alias("first_score"),
        F.first("enrolled_at", ignorenulls=True).over(w_student_major_grade_status_b_2).alias("first_known_enrolled_at"),
        F.dense_rank().over(w_student_major_student_cohort).alias("enrolled_at_dense_rank"),
        F.round(F.col("student_gpa") - F.avg("student_gpa").over(w_enrolled_at_to_date), 2).alias("student_gpa_vs_enrolled_at_to_date_avg"),
        F.min("score").over(w_enrolled_at_to_date).alias("enrolled_at_to_date_min_score"),
        F.lead("enrolled_at").over(w_student_id_by_enrolled_at).alias("next_enrolled_at"),
        F.count(F.lit(1)).over(w_student_id).alias("rows_in_student_id"),
        F.size(F.collect_set("student_major").over(w_student_id)).alias("distinct_student_major_in_student_id"),
        F.round(F.cume_dist().over(w_all_by_score_ties), 3).alias("score_cume_dist"),
    )
)

with_windows = (
    windowed
    .select(
        "*",
        F.nth_value("score_sum_3", 2).over(w_student_major_by_score_cume).alias("second_score_sum_3"),
        F.round(F.cume_dist().over(w_student_major_by_score_cume_2), 3).alias("score_cume_dist_cume_dist"),
        F.sum("student_gpa_change").over(w_distinct_student_major_in_st).alias("distinct_student_major_in_student_id_sum_student"),
        F.count(F.lit(1)).over(w_distinct_student_major_in_st).alias("rows_in_distinct_student_major_in_student_id"),
        F.row_number().over(w_all_by_first_score).alias("rn"),
        F.row_number().over(w_student_major_by_score_cume_3).alias("rn_2"),
    )
)

result = (
    with_windows
    .filter(F.col("score_cume_dist_cume_dist") >= 0.2)
    .select(
        "id",
        "course_id",
        "enrolled_at",
        "student_major",
        "student_cohort",
        "student_gpa",
        "course_code",
        "seq_no",
        "score_sum_3",
        "student_gpa_change",
        "score_percent_rank",
        "first_score",
        "first_known_enrolled_at",
        "student_gpa_vs_enrolled_at_to_date_avg",
        "enrolled_at_to_date_min_score",
        "next_enrolled_at",
        "rows_in_student_id",
        "score_cume_dist",
        "second_score_sum_3",
        "distinct_student_major_in_student_id_sum_student",
        "rows_in_distinct_student_major_in_student_id",
        "rn",
        "rn_2",
        F.concat_ws("|", F.col("student_cohort"), F.col("grade_status")).alias("student_cohort_grade_status_label"),
        F.greatest(F.col("score_sum_3"), F.col("seq_no")).alias("greatest_score_sum_3_seq_no"),
    )
    .orderBy(
        F.col("distinct_student_major_in_student_id_sum_student").desc(),
        "rows_in_distinct_student_major_in_student_id",
    )
)
