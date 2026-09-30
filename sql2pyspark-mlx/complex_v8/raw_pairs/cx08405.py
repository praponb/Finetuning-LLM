from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_student_is_international_by = Window.partitionBy("student_is_international").orderBy("score", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_all_by_score_full = Window.orderBy(F.col("score").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_all_by_score = Window.orderBy(F.col("score").desc(), "id")
w_all_by_score_running = Window.orderBy(F.col("score").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_student_major_student_id_by = Window.partitionBy("student_major", "student_id").orderBy("enrolled_at", "id")
w_student_is_international_stu = Window.partitionBy("student_is_international", "student_major").orderBy("enrolled_at", "id")
w_student_is_international_by_2 = Window.partitionBy("student_is_international").orderBy(F.col("enrolled_at").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_term_grade_status = Window.partitionBy("term", "grade_status")
w_student_is_international_stu_2 = Window.partitionBy("student_is_international", "student_major").orderBy("enrolled_at", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

prepared_enrollments = (
    df_enrollments
    .filter(F.col("student_id").isNotNull())
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
    prepared_enrollments.alias("s")
    .join(df_students.alias("st"), (F.col("st.id") == F.col("s.student_id")) & (F.col("st.major") == "CS"), "inner")
    .select(
        "s.id",
        "s.student_id",
        "s.course_id",
        "s.term",
        "s.score",
        "s.grade_status",
        "s.enrolled_at",
        F.col("st.major").alias("student_major"),
        F.col("st.cohort").alias("student_cohort"),
        F.col("st.is_international").alias("student_is_international"),
    )
)

windowed = (
    combined_details
    .select(
        "*",
        F.first("score", ignorenulls=True).over(w_student_is_international_by).alias("first_known_score"),
        F.nth_value("enrolled_at", 2).over(w_all_by_score_full).alias("second_enrolled_at"),
        F.lead("enrolled_at").over(w_all_by_score).alias("next_enrolled_at"),
        F.last("score", ignorenulls=True).over(w_all_by_score_running).alias("last_known_score"),
        F.lag("score").over(w_student_major_student_id_by).alias("prev_score"),
        F.lead("score").over(w_student_major_student_id_by).alias("next_score"),
        F.row_number().over(w_student_is_international_stu).alias("enrolled_at_seq"),
        F.first("score").over(w_student_is_international_by_2).alias("first_score"),
        F.size(F.collect_set("course_id").over(w_term_grade_status)).alias("distinct_course_id_in_term_grade_status"),
        F.max("score").over(w_student_is_international_stu_2).alias("max_score_so_far"),
    )
)

result = (
    windowed
    .filter(F.col("first_known_score") <= 71.0)
    .select(
        "id",
        "student_id",
        "score",
        "grade_status",
        "enrolled_at",
        "student_major",
        "student_cohort",
        "second_enrolled_at",
        "next_enrolled_at",
        "prev_score",
        "next_score",
        "first_score",
        "distinct_course_id_in_term_grade_status",
        "max_score_so_far",
        F.round((F.unix_timestamp(F.col("next_enrolled_at")) - F.unix_timestamp(F.col("second_enrolled_at"))) / 3600, 1).alias("hours_second_enrolled_at_to_next_enrolled_at"),
        F.to_date(F.col("next_enrolled_at")).alias("next_enrolled_at_date"),
    )
    .orderBy("student_id", F.col("score").asc_nulls_last(), "id")
    .limit(10)
)
