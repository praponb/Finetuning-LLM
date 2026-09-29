from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_course_id_grade_status_by_en = Window.partitionBy("course_id", "grade_status").orderBy("enrolled_at_date", "id")
w_student_id_grade_status_by_e = Window.partitionBy("student_id", "grade_status").orderBy("enrolled_at_date")
w_enrolled_at_date_year_course = Window.partitionBy(F.year("enrolled_at_date"), "course_id").orderBy(F.col("enrolled_at_date").desc(), "id")
w_student_id_grade_status_by_e_2 = Window.partitionBy("student_id", "grade_status").orderBy("enrolled_at_date", "id")
w_course_level_by_enrolled_at = Window.partitionBy("course_level").orderBy(F.col("enrolled_at_date").desc(), "id")
w_course_id_grade_status_by_en_2 = Window.partitionBy("course_id", "grade_status").orderBy("enrolled_at_date")
w_enrolled_at_date_month_by_en = Window.partitionBy(F.month("enrolled_at_date")).orderBy("enrolled_at_date", "id")
w_enrolled_at_date_year_course_2 = Window.partitionBy(F.year("enrolled_at_date"), "course_id").orderBy(F.col("enrolled_at_date").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_second_enrolled_at_date_mont = Window.partitionBy(F.date_trunc("month", "second_enrolled_at_date")).orderBy("prev2_enrolled_at_date", "id")
w_enrolled_at_date_dense_rank = Window.partitionBy("enrolled_at_date_dense_rank")
w_term_by_next2_enrolled_at_ti = Window.partitionBy("term").orderBy("next2_enrolled_at")
w_second_enrolled_at_date_mont_2 = Window.partitionBy(F.date_trunc("month", "second_enrolled_at_date")).orderBy("prev2_enrolled_at_date", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_all_by_next2_enrolled_at = Window.orderBy("next2_enrolled_at", "id")

scoped = (
    df_enrollments
    .filter((F.col("enrolled_at") >= F.lit("2025-02-06 09:00:00").cast("timestamp")) & (F.col("student_id") >= 13))
    .select(
        "id",
        "student_id",
        "course_id",
        "term",
        "score",
        "grade_status",
        "enrolled_at",
        F.concat_ws("-", F.col("term"), F.col("grade_status")).alias("term_grade_status_label"),
        F.to_date(F.col("enrolled_at")).alias("enrolled_at_date"),
    )
)

joined_student = (
    scoped.alias("s")
    .join(df_students.alias("st"), (F.col("st.id") == F.col("s.student_id")) & (F.col("st.cohort") == "C2024"), "left")
    .join(df_courses.alias("c"), F.col("c.id") == F.col("s.course_id"), "left")
    .select(
        "s.id",
        "s.student_id",
        "s.course_id",
        "s.term",
        "s.grade_status",
        "s.enrolled_at",
        "s.term_grade_status_label",
        "s.enrolled_at_date",
        F.col("st.cohort").alias("student_cohort"),
        "st.student_name",
        F.col("c.level").alias("course_level"),
    )
)

with_windows = (
    joined_student
    .select(
        "id",
        "term",
        "grade_status",
        "enrolled_at",
        "term_grade_status_label",
        "student_name",
        "course_level",
        F.round((F.unix_timestamp("enrolled_at") - F.unix_timestamp(F.lag("enrolled_at").over(w_course_id_grade_status_by_en))) / 60, 1).alias("minutes_since_prev_enrolled_at"),
        F.round(F.cume_dist().over(w_student_id_grade_status_by_e), 3).alias("enrolled_at_date_cume_dist"),
        F.lead("enrolled_at", 2).over(w_enrolled_at_date_year_course).alias("next2_enrolled_at"),
        F.ntile(3).over(w_student_id_grade_status_by_e_2).alias("enrolled_at_date_tercile"),
        F.round(F.percent_rank().over(w_course_level_by_enrolled_at), 3).alias("enrolled_at_date_percent_rank"),
        F.round(F.percent_rank().over(w_course_id_grade_status_by_en_2), 3).alias("enrolled_at_date_percent_rank_2"),
        F.round(F.percent_rank().over(w_enrolled_at_date_year_course), 3).alias("enrolled_at_date_percent_rank_3"),
        F.row_number().over(w_course_id_grade_status_by_en).alias("seq_no"),
        F.lag("enrolled_at").over(w_enrolled_at_date_year_course).alias("prev_enrolled_at"),
        F.lag("enrolled_at", 2).over(w_enrolled_at_date_month_by_en).alias("prev2_enrolled_at"),
        F.dense_rank().over(w_enrolled_at_date_year_course).alias("enrolled_at_date_dense_rank"),
        F.lag("enrolled_at_date", 2).over(w_course_id_grade_status_by_en).alias("prev2_enrolled_at_date"),
        F.nth_value("enrolled_at_date", 2).over(w_enrolled_at_date_year_course_2).alias("second_enrolled_at_date"),
    )
)

with_windows_2 = (
    with_windows
    .select(
        "id",
        "term",
        "grade_status",
        "enrolled_at",
        "term_grade_status_label",
        "student_name",
        "course_level",
        "minutes_since_prev_enrolled_at",
        "enrolled_at_date_cume_dist",
        "next2_enrolled_at",
        "enrolled_at_date_percent_rank",
        "enrolled_at_date_percent_rank_2",
        "enrolled_at_date_percent_rank_3",
        "seq_no",
        "prev_enrolled_at",
        "prev2_enrolled_at",
        "prev2_enrolled_at_date",
        F.datediff(F.col("second_enrolled_at_date"), F.lag("second_enrolled_at_date").over(w_second_enrolled_at_date_mont)).alias("days_since_prev_second_enrolled_at_date"),
        F.round(F.col("enrolled_at_date_tercile") / F.nullif(F.sum("enrolled_at_date_tercile").over(w_enrolled_at_date_dense_rank), F.lit(0)), 4).alias("enrolled_at_date_tercile_share_of_enrolled_at_da"),
        F.rank().over(w_term_by_next2_enrolled_at_ti).alias("next2_enrolled_at_rank"),
        F.max("seq_no").over(w_second_enrolled_at_date_mont_2).alias("max_seq_no_so_far"),
        F.lead("seq_no", 3, 0).over(w_all_by_next2_enrolled_at).alias("next3_seq_no"),
        F.row_number().over(w_second_enrolled_at_date_mont).alias("seq_no_2"),
        F.round((F.unix_timestamp("prev_enrolled_at") - F.unix_timestamp(F.lag("prev_enrolled_at").over(w_second_enrolled_at_date_mont))) / 60, 1).alias("minutes_since_prev_prev_enrolled_at"),
        F.size(F.collect_set("grade_status").over(w_enrolled_at_date_dense_rank)).alias("distinct_grade_status_in_enrolled_at_date_dense_"),
    )
)

result = (
    with_windows_2
    .select(
        "id",
        "term",
        "enrolled_at",
        "term_grade_status_label",
        "student_name",
        "enrolled_at_date_cume_dist",
        "next2_enrolled_at",
        "enrolled_at_date_percent_rank_2",
        "enrolled_at_date_percent_rank_3",
        "seq_no",
        "prev_enrolled_at",
        "prev2_enrolled_at",
        "prev2_enrolled_at_date",
        "days_since_prev_second_enrolled_at_date",
        "max_seq_no_so_far",
        "next3_seq_no",
        "seq_no_2",
        "minutes_since_prev_prev_enrolled_at",
        "distinct_grade_status_in_enrolled_at_date_dense_",
        F.concat(F.col("student_name"), F.lit("#"), F.col("next2_enrolled_at_rank").cast("string")).alias("student_name_ref"),
        F.coalesce(F.col("next3_seq_no"), F.lit(0)).alias("next3_seq_no_filled"),
    )
    .orderBy(F.col("next3_seq_no_filled").desc(), "id")
    .limit(20)
)
