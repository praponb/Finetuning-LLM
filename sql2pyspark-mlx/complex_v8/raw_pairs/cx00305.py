from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_student_id_by_student_enroll = Window.partitionBy("student_id").orderBy(F.col("student_enrolled_on").desc(), "id", "enrollment_id")
w_student_id_by_student_enroll_2 = Window.partitionBy("student_id").orderBy(F.col("student_enrolled_on").desc(), "id", "enrollment_id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)

filtered = (
    df_enrollments
    .filter((~F.col("term").isin("2025T1", "2025T2")) & (~F.col("grade_status").isin("INCOMPLETE", "PASS")))
    .select(
        "id",
        "student_id",
        "course_id",
        "term",
        "score",
        "grade_status",
        "enrolled_at",
        F.abs(F.col("score") - 49.0).alias("score_distance"),
        F.concat(F.col("grade_status"), F.lit("#"), F.col("id").cast("string")).alias("grade_status_ref"),
        F.substring(F.trim(F.col("term")), 1, 4).alias("term_prefix"),
    )
)

with_children_enrollments = (
    filtered.alias("s")
    .join(df_enrollments.alias("e"), F.col("e.student_id") == F.col("s.student_id"), "left")
    .select(
        "s.id",
        "s.student_id",
        "s.course_id",
        "s.term",
        "s.grade_status_ref",
        "s.term_prefix",
        F.col("e.id").alias("enrollment_id"),
        F.col("e.grade_status").alias("enrollment_grade_status"),
        F.col("e.term").alias("enrollment_term"),
    )
)

combined_details = (
    with_children_enrollments.alias("s")
    .join(df_courses.alias("c"), F.col("c.id") == F.col("s.course_id"), "inner")
    .join(df_students.alias("st"), F.col("st.id") == F.col("s.student_id"), "inner")
    .select(
        "s.*",
        "c.course_code",
        F.col("c.dept").alias("course_dept"),
        F.col("c.level").alias("course_level"),
        F.col("st.enrolled_on").alias("student_enrolled_on"),
        F.col("st.is_international").alias("student_is_international"),
    )
)

sequenced = (
    combined_details
    .select(
        "id",
        "student_id",
        "course_id",
        "grade_status_ref",
        "enrollment_id",
        "enrollment_grade_status",
        "course_code",
        "course_dept",
        "course_level",
        "student_is_international",
        F.lag("student_enrolled_on", 3).over(w_student_id_by_student_enroll).alias("prev3_student_enrolled_on"),
        F.nth_value("student_enrolled_on", 2).over(w_student_id_by_student_enroll_2).alias("second_student_enrolled_on"),
        F.lag("student_enrolled_on").over(w_student_id_by_student_enroll).alias("prev_student_enrolled_on"),
    )
)

result = (
    sequenced
    .groupingSets(
        [["course_level", "enrollment_grade_status", "course_id"], ["course_level"], []],
        "course_level", "enrollment_grade_status", "course_id",
    )
    .agg(
        F.min("prev_student_enrolled_on").alias("first_prev_student_enrolled_on"),
        F.count(F.when(F.length(F.trim(F.col("course_code"))) >= 7, 1)).alias("n_length_trim_course_code_7"),
    )
    .select(
        F.coalesce(F.col("course_level"), F.lit("ALL")).alias("course_level"),
        F.coalesce(F.col("enrollment_grade_status"), F.lit("ALL")).alias("enrollment_grade_status"),
        "course_id",
        "first_prev_student_enrolled_on",
        "n_length_trim_course_code_7",
    )
    .orderBy(
        F.col("course_level").asc_nulls_last(),
        F.col("enrollment_grade_status").asc_nulls_last(),
        F.col("course_id").asc_nulls_last(),
    )
)
