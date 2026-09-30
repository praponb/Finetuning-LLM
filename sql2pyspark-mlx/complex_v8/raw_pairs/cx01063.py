from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_course_id_by_score_prior_3 = Window.partitionBy("course_id").orderBy(F.col("score").asc_nulls_last(), "id").rowsBetween(-3, -1)
w_student_is_international_enr = Window.partitionBy("student_is_international", F.year("enrolled_at"))
w_course_id_by_score = Window.partitionBy("course_id").orderBy(F.col("score").asc_nulls_last(), "id")
w_all_by_score = Window.orderBy(F.col("score").desc(), "id")
w_all_by_enrolled_at_moving_2 = Window.orderBy("enrolled_at", "id").rowsBetween(-2, Window.currentRow)
w_all_by_enrolled_at = Window.orderBy("enrolled_at", "id")

recent = (
    df_enrollments
    .filter(F.col("course_id") <= 11)
    .select(
        "id",
        "student_id",
        "course_id",
        "term",
        "score",
        "grade_status",
        "enrolled_at",
        F.substring(F.trim(F.col("grade_status")), 1, 3).alias("grade_status_prefix"),
        F.date_format(F.col("enrolled_at"), "yyyy-MM").alias("enrolled_at_label"),
    )
)

combined_details_student = (
    recent.alias("s")
    .join(df_students.alias("st"), F.col("st.id") == F.col("s.student_id"), "inner")
    .join(df_courses.alias("c"), F.col("c.id") == F.col("s.course_id"), "left")
    .filter(~F.col("st.is_international"))
    .select(
        "s.id",
        "s.student_id",
        "s.course_id",
        "s.score",
        "s.enrolled_at",
        "s.enrolled_at_label",
        F.col("st.is_international").alias("student_is_international"),
        "c.course_code",
    )
)

windowed = (
    combined_details_student
    .select(
        "*",
        F.max("score").over(w_course_id_by_score_prior_3).alias("score_max_3_prior"),
        F.round(F.col("score") - F.avg("score").over(w_student_is_international_enr), 2).alias("score_vs_student_is_international_enrolled_at_ye"),
        F.lag("enrolled_at").over(w_course_id_by_score).alias("prev_enrolled_at"),
        F.lead("enrolled_at", 3).over(w_all_by_score).alias("next3_enrolled_at"),
        F.count(F.lit(1)).over(w_student_is_international_enr).alias("rows_in_student_is_international_enrolled_at_yea"),
        F.max("score").over(w_all_by_enrolled_at_moving_2).alias("score_max_3"),
        F.row_number().over(w_all_by_enrolled_at).alias("enrolled_at_seq"),
    )
)

result = (
    windowed
    .filter(F.col("score_max_3") < 270.0)
    .select(
        "id",
        "student_id",
        "score",
        "enrolled_at",
        "enrolled_at_label",
        "course_code",
        "score_vs_student_is_international_enrolled_at_ye",
        "prev_enrolled_at",
        "rows_in_student_is_international_enrolled_at_yea",
        "score_max_3",
        "enrolled_at_seq",
        F.datediff(F.lit("2026-01-01").cast("date"), F.col("enrolled_at")).alias("days_since_enrolled_at"),
        F.round((F.unix_timestamp(F.col("prev_enrolled_at")) - F.unix_timestamp(F.col("enrolled_at"))) / 3600, 1).alias("hours_enrolled_at_to_prev_enrolled_at"),
    )
    .orderBy(
        F.col("rows_in_student_is_international_enrolled_at_yea").asc(),
        F.col("hours_enrolled_at_to_prev_enrolled_at").desc(),
    )
)
