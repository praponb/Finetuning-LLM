from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_all_by_enrolled_at_ties = Window.orderBy("enrolled_at")
w_course_id_student_major_by_s = Window.partitionBy("course_id", "student_major").orderBy(F.col("student_enrolled_on").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_grade_status_student_id_by_s = Window.partitionBy("grade_status", "student_id").orderBy(F.col("student_enrolled_on").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_grade_status_student_id_by_s_2 = Window.partitionBy("grade_status", "student_id").orderBy(F.col("student_enrolled_on").desc(), "id")
w_student_major_student_enroll = Window.partitionBy("student_major", F.year("student_enrolled_on")).orderBy("score", "id").rowsBetween(-2, Window.currentRow)
w_all_by_enrolled_at_running = Window.orderBy("enrolled_at", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_all_by_student_enrolled_on_f = Window.orderBy(F.col("student_enrolled_on").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_student_id = Window.partitionBy("student_id")
w_all_by_enrolled_at_moving_3 = Window.orderBy("enrolled_at", "id").rowsBetween(-3, Window.currentRow)
w_student_major_student_enroll_2 = Window.partitionBy("student_major", F.year("student_enrolled_on")).orderBy("score", "id")

filtered_enrollments = (
    df_enrollments
    .filter(F.col("term") == "2024T2")
    .select(
        "id",
        "student_id",
        "course_id",
        "score",
        "grade_status",
        "enrolled_at",
    )
)

with_details = (
    filtered_enrollments.alias("s")
    .join(df_students.alias("st"), (F.col("st.id") == F.col("s.student_id")) & (F.col("st.cohort").isNotNull()), "left")
    .select(
        "s.*",
        F.col("st.major").alias("student_major"),
        F.col("st.enrolled_on").alias("student_enrolled_on"),
    )
)

sequenced = (
    with_details
    .select(
        "id",
        "score",
        "grade_status",
        "enrolled_at",
        "student_enrolled_on",
        F.round(F.cume_dist().over(w_all_by_enrolled_at_ties), 3).alias("enrolled_at_cume_dist"),
        F.nth_value("score", 2).over(w_course_id_student_major_by_s).alias("second_score"),
        F.nth_value("score", 2).over(w_grade_status_student_id_by_s).alias("second_score_2"),
        F.round((F.unix_timestamp("enrolled_at") - F.unix_timestamp(F.lag("enrolled_at").over(w_grade_status_student_id_by_s_2))) / 60, 1).alias("minutes_since_prev_enrolled_at"),
        F.max("score").over(w_student_major_student_enroll).alias("score_max_3"),
        F.first("enrolled_at", ignorenulls=True).over(w_all_by_enrolled_at_running).alias("first_known_enrolled_at"),
        F.last("enrolled_at").over(w_all_by_student_enrolled_on_f).alias("last_enrolled_at"),
        F.round(F.col("score") / F.nullif(F.sum("score").over(w_student_id), F.lit(0)), 4).alias("score_share_of_student_id"),
        F.sum("score").over(w_all_by_enrolled_at_moving_3).alias("score_sum_4"),
        F.row_number().over(w_student_major_student_enroll_2).alias("seq_no"),
    )
)

top_rows = (
    sequenced
    .filter(F.col("seq_no") == 1)
    .drop("seq_no")
)

result = (
    top_rows
    .filter(F.col("grade_status") == "PASS")
    .select(
        "id",
        "score",
        "grade_status",
        "enrolled_at",
        "student_enrolled_on",
        "enrolled_at_cume_dist",
        "second_score",
        "second_score_2",
        "minutes_since_prev_enrolled_at",
        "score_max_3",
        "first_known_enrolled_at",
        "score_share_of_student_id",
        "score_sum_4",
        (F.col("score_max_3") - F.col("second_score_2")).alias("score_max_3_minus_second_score_2"),
        F.round(F.col("enrolled_at_cume_dist") * 1.1, 2).alias("enrolled_at_cume_dist_adj"),
    )
    .orderBy(F.col("enrolled_at_cume_dist").asc())
)
