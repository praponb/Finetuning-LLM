from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_course_id_grade_status_by_en = Window.partitionBy("course_id", "grade_status").orderBy("enrolled_at", "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_student_cohort = Window.partitionBy("student_cohort")
w_grade_status_student_major_b = Window.partitionBy("grade_status", "student_major").orderBy(F.col("score_distance").asc_nulls_last())
w_all_by_enrolled_at_full = Window.orderBy(F.col("enrolled_at").desc_nulls_first(), "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_grade_status_student_major_b_2 = Window.partitionBy("grade_status", "student_major").orderBy(F.col("score_distance").asc_nulls_last(), "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_all_by_enrolled_at_ties = Window.orderBy(F.col("enrolled_at").desc_nulls_first())
w_all_by_enrolled_at_running = Window.orderBy(F.col("enrolled_at").desc_nulls_first(), "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_all_by_student_cohort_max_sc = Window.orderBy("student_cohort_max_score", "id")
w_student_id_first_known_stude = Window.partitionBy("student_id", F.month("first_known_student_enrolled_on")).orderBy(F.col("score_distance_cume_dist").desc(), "id")
w_course_id = Window.partitionBy("course_id")
w_all_by_student_cohort_max_sc_2 = Window.orderBy("student_cohort_max_score", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_student_cohort_course_id_by = Window.partitionBy("student_cohort", "course_id").orderBy(F.col("first_known_student_enrolled_on").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

valid_enrollments = (
    df_enrollments
    .filter((F.to_date(F.col("enrolled_at")) < F.lit("2025-02-18").cast("date")) & (F.col("course_id") >= 7))
    .select(
        "id",
        "student_id",
        "course_id",
        "term",
        "score",
        "grade_status",
        "enrolled_at",
        F.abs(F.col("score") - 34.2).alias("score_distance"),
    )
)

joined = (
    valid_enrollments.alias("s")
    .join(df_students.alias("st"), F.col("st.id") == F.col("s.student_id"), "inner")
    .filter(F.col("st.enrolled_on").between(F.lit("2024-01-24").cast("date"), F.lit("2025-07-18").cast("date")))
    .select(
        "s.id",
        "s.student_id",
        "s.course_id",
        "s.score",
        "s.grade_status",
        "s.enrolled_at",
        "s.score_distance",
        F.col("st.cohort").alias("student_cohort"),
        F.col("st.enrolled_on").alias("student_enrolled_on"),
        F.col("st.major").alias("student_major"),
    )
)

with_windows = (
    joined
    .select(
        "id",
        "student_id",
        "course_id",
        "score",
        "grade_status",
        "enrolled_at",
        "score_distance",
        "student_cohort",
        "student_enrolled_on",
        F.nth_value("enrolled_at", 2).over(w_course_id_grade_status_by_en).alias("second_enrolled_at"),
        F.round(F.avg("score_distance").over(w_student_cohort), 2).alias("student_cohort_avg_score_distance"),
        F.max("score").over(w_student_cohort).alias("student_cohort_max_score"),
        F.round(F.cume_dist().over(w_grade_status_student_major_b), 3).alias("score_distance_cume_dist"),
        F.nth_value("enrolled_at", 2).over(w_all_by_enrolled_at_full).alias("second_enrolled_at_2"),
        F.round(F.col("score") - F.avg("score").over(w_student_cohort), 2).alias("score_vs_student_cohort_avg"),
        F.size(F.collect_set("student_major").over(w_student_cohort)).alias("distinct_student_major_in_student_cohort"),
        F.round(F.col("score_distance") - F.avg("score_distance").over(w_student_cohort), 2).alias("score_distance_vs_student_cohort_avg"),
        F.sum("score").over(w_student_cohort).alias("student_cohort_sum_score"),
        F.count(F.lit(1)).over(w_student_cohort).alias("rows_in_student_cohort"),
        F.first("score", ignorenulls=True).over(w_grade_status_student_major_b_2).alias("first_known_score"),
        F.dense_rank().over(w_all_by_enrolled_at_ties).alias("enrolled_at_dense_rank"),
        F.first("student_enrolled_on", ignorenulls=True).over(w_all_by_enrolled_at_running).alias("first_known_student_enrolled_on"),
    )
)

with_trends = (
    with_windows
    .select(
        "id",
        "student_id",
        "grade_status",
        "enrolled_at",
        "score_distance",
        "student_enrolled_on",
        "second_enrolled_at",
        "student_cohort_avg_score_distance",
        "student_cohort_max_score",
        "second_enrolled_at_2",
        "score_vs_student_cohort_avg",
        "distinct_student_major_in_student_cohort",
        "score_distance_vs_student_cohort_avg",
        "student_cohort_sum_score",
        "rows_in_student_cohort",
        "first_known_score",
        "enrolled_at_dense_rank",
        "first_known_student_enrolled_on",
        F.row_number().over(w_all_by_student_cohort_max_sc).alias("student_cohort_max_score_seq"),
        F.round(F.cume_dist().over(w_student_id_first_known_stude), 3).alias("score_distance_cume_dist_cume_dist"),
        F.count(F.lit(1)).over(w_course_id).alias("rows_in_course_id"),
        (F.col("score") - F.lag("score").over(w_student_id_first_known_stude)).alias("score_change"),
        F.dense_rank().over(w_all_by_student_cohort_max_sc).alias("student_cohort_max_score_dense_rank"),
        F.min("first_known_score").over(w_all_by_student_cohort_max_sc_2).alias("min_first_known_score_so_far"),
        F.last("enrolled_at_dense_rank", ignorenulls=True).over(w_student_cohort_course_id_by).alias("last_known_enrolled_at_dense_rank"),
    )
)

result = (
    with_trends
    .select(
        "id",
        "student_id",
        "grade_status",
        "enrolled_at",
        "score_distance",
        "student_enrolled_on",
        "second_enrolled_at",
        "student_cohort_avg_score_distance",
        "student_cohort_max_score",
        "second_enrolled_at_2",
        "score_vs_student_cohort_avg",
        "distinct_student_major_in_student_cohort",
        "score_distance_vs_student_cohort_avg",
        "student_cohort_sum_score",
        "rows_in_student_cohort",
        "enrolled_at_dense_rank",
        "student_cohort_max_score_seq",
        "score_distance_cume_dist_cume_dist",
        "score_change",
        "min_first_known_score_so_far",
        "last_known_enrolled_at_dense_rank",
        F.when(F.col("score_change") >= 10.0, "large").when(F.col("score_change") >= -40.0, "medium").otherwise("small").alias("score_change_tier"),
    )
    .orderBy(F.col("grade_status").desc())
)
