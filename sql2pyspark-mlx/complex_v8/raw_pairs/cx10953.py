from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_course_id_student_id_by_stud = Window.partitionBy("course_id", "student_id").orderBy(F.col("student_enrolled_on").desc())
w_grade_status_by_enrolled_at = Window.partitionBy("grade_status").orderBy("enrolled_at", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_grade_status_by_enrolled_at_2 = Window.partitionBy("grade_status").orderBy("enrolled_at", "id")
w_all_by_enrolled_at = Window.orderBy("enrolled_at", "id")
w_course_id_by_student_enrolle = Window.partitionBy("course_id").orderBy("student_enrolled_on", "id")
w_all_by_enrolled_at_full = Window.orderBy("enrolled_at", "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_course_id_student_id_by_stud_2 = Window.partitionBy("course_id", "student_id").orderBy(F.col("student_enrolled_on").desc(), "id")
w_course_id_enrolled_at_quarti = Window.partitionBy("course_id", "enrolled_at_quartile")
w_enrolled_at_decile_second_st = Window.partitionBy("enrolled_at_decile", F.date_trunc("month", "second_student_enrolled_on")).orderBy("student_enrolled_on_percent_rank", "id")
w_enrolled_at_decile_second_st_2 = Window.partitionBy("enrolled_at_decile", F.date_trunc("month", "second_student_enrolled_on")).orderBy("student_enrolled_on_percent_rank")
w_enrolled_at_decile_second_st_3 = Window.partitionBy("enrolled_at_decile", F.date_trunc("month", "second_student_enrolled_on")).orderBy("student_enrolled_on_percent_rank", "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)

filtered_enrollments = (
    df_enrollments
    .filter((F.col("grade_status").isNotNull()) & (F.col("term") != "2024T2"))
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

combined_details = (
    filtered_enrollments.alias("s")
    .join(df_students.alias("st"), F.col("st.id") == F.col("s.student_id"), "inner")
    .select(
        "s.id",
        "s.student_id",
        "s.course_id",
        "s.grade_status",
        "s.enrolled_at",
        "st.student_name",
        F.col("st.enrolled_on").alias("student_enrolled_on"),
    )
)

windowed = (
    combined_details
    .select(
        "*",
        F.dense_rank().over(w_course_id_student_id_by_stud).alias("student_enrolled_on_dense_rank"),
        F.first("enrolled_at", ignorenulls=True).over(w_grade_status_by_enrolled_at).alias("first_known_enrolled_at"),
        F.dense_rank().over(w_grade_status_by_enrolled_at_2).alias("enrolled_at_dense_rank"),
        F.round((F.unix_timestamp("enrolled_at") - F.unix_timestamp(F.lag("enrolled_at").over(w_all_by_enrolled_at))) / 60, 1).alias("minutes_since_prev_enrolled_at"),
        F.ntile(10).over(w_all_by_enrolled_at).alias("enrolled_at_decile"),
        F.rank().over(w_all_by_enrolled_at).alias("enrolled_at_rank"),
        F.datediff(F.col("student_enrolled_on"), F.lag("student_enrolled_on").over(w_course_id_by_student_enrolle)).alias("days_since_prev_student_enrolled_on"),
        F.last("enrolled_at", ignorenulls=True).over(w_grade_status_by_enrolled_at).alias("last_known_enrolled_at"),
        F.round(F.percent_rank().over(w_course_id_student_id_by_stud), 3).alias("student_enrolled_on_percent_rank"),
        F.nth_value("enrolled_at", 2).over(w_all_by_enrolled_at_full).alias("second_enrolled_at"),
        F.ntile(4).over(w_grade_status_by_enrolled_at_2).alias("enrolled_at_quartile"),
        F.round(F.cume_dist().over(w_grade_status_by_enrolled_at_2), 3).alias("enrolled_at_cume_dist"),
        F.nth_value("student_enrolled_on", 2).over(w_all_by_enrolled_at_full).alias("second_student_enrolled_on"),
        F.ntile(5).over(w_course_id_student_id_by_stud_2).alias("student_enrolled_on_quintile"),
    )
)

with_windows = (
    windowed
    .select(
        "id",
        "student_id",
        "course_id",
        "grade_status",
        "enrolled_at",
        "student_name",
        "student_enrolled_on_dense_rank",
        "enrolled_at_dense_rank",
        "minutes_since_prev_enrolled_at",
        "enrolled_at_decile",
        "days_since_prev_student_enrolled_on",
        "student_enrolled_on_percent_rank",
        "enrolled_at_cume_dist",
        "second_student_enrolled_on",
        "student_enrolled_on_quintile",
        F.max("student_enrolled_on_quintile").over(w_course_id_enrolled_at_quarti).alias("course_id_enrolled_at_quartile_max_student_enrol"),
        F.round((F.unix_timestamp("second_enrolled_at") - F.unix_timestamp(F.lag("second_enrolled_at").over(w_enrolled_at_decile_second_st))) / 60, 1).alias("minutes_since_prev_second_enrolled_at"),
        F.size(F.collect_set("student_id").over(w_course_id_enrolled_at_quarti)).alias("distinct_student_id_in_course_id_enrolled_at_qua"),
        F.round(F.col("enrolled_at_dense_rank") / F.nullif(F.sum("enrolled_at_dense_rank").over(w_course_id_enrolled_at_quarti), F.lit(0)), 4).alias("enrolled_at_dense_rank_share_of_course_id_enroll"),
        F.round(F.cume_dist().over(w_enrolled_at_decile_second_st_2), 3).alias("student_enrolled_on_percent_rank_cume_dist"),
        F.last("last_known_enrolled_at").over(w_enrolled_at_decile_second_st_3).alias("last_last_known_enrolled_at"),
        F.round(F.col("enrolled_at_rank") / F.nullif(F.sum("enrolled_at_rank").over(w_course_id_enrolled_at_quarti), F.lit(0)), 4).alias("enrolled_at_rank_share_of_course_id_enrolled_at_"),
    )
)

result = (
    with_windows
    .select(
        "id",
        "student_id",
        "course_id",
        "grade_status",
        "enrolled_at",
        "student_name",
        "student_enrolled_on_dense_rank",
        "enrolled_at_dense_rank",
        "minutes_since_prev_enrolled_at",
        "enrolled_at_decile",
        "days_since_prev_student_enrolled_on",
        "student_enrolled_on_percent_rank",
        "enrolled_at_cume_dist",
        "second_student_enrolled_on",
        "student_enrolled_on_quintile",
        "minutes_since_prev_second_enrolled_at",
        "distinct_student_id_in_course_id_enrolled_at_qua",
        "enrolled_at_dense_rank_share_of_course_id_enroll",
        "enrolled_at_rank_share_of_course_id_enrolled_at_",
        F.datediff(F.lit("2025-06-30").cast("date"), F.col("enrolled_at")).alias("days_since_enrolled_at"),
        F.datediff(F.lit("2025-06-30").cast("date"), F.col("enrolled_at")).alias("days_since_enrolled_at_2"),
    )
    .orderBy(F.col("enrolled_at_dense_rank").asc_nulls_last(), "id")
    .limit(20)
)
