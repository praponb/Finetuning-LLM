from pyspark.sql import functions as F

prepared_enrollments = (
    df_enrollments
    .filter(F.col("student_id") >= 4)
    .select(
        "id",
        "student_id",
        "course_id",
        "term",
        "score",
        "grade_status",
        F.coalesce(F.col("score"), F.lit(0.0)).alias("score_filled"),
        F.round(F.col("score") * 1.1, 2).alias("score_adj"),
        F.concat_ws(" / ", F.col("grade_status"), F.col("term")).alias("grade_status_term_label"),
    )
)

enrollment_stats = (
    df_enrollments
    .groupBy("student_id")
    .agg(
        F.min("enrolled_at").alias("first_enrollment_enrolled_at"),
        F.count("*").alias("enrollment_count"),
    )
)

summarized_rows = (
    prepared_enrollments.alias("s")
    .join(enrollment_stats.alias("e"), F.col("e.student_id") == F.col("s.student_id"), "left")
    .select(
        "s.*",
        "e.first_enrollment_enrolled_at",
        F.coalesce(F.col("e.enrollment_count"), F.lit(0)).alias("enrollment_count"),
    )
)

enrollment_stats_2 = (
    df_enrollments
    .groupBy("course_id")
    .agg(
        F.count(F.when(F.col("term") != "2025T1", 1)).alias("enrollment_term_count"),
        F.count("*").alias("enrollment_count_2"),
    )
)

with_stats = (
    summarized_rows.alias("s")
    .join(enrollment_stats_2.alias("e"), F.col("e.course_id") == F.col("s.course_id"), "left")
    .select(
        "s.*",
        F.coalesce(F.col("e.enrollment_term_count"), F.lit(0)).alias("enrollment_term_count"),
        F.coalesce(F.col("e.enrollment_count_2"), F.lit(0)).alias("enrollment_count_2"),
    )
)

with_details = (
    with_stats.alias("s")
    .join(df_students.alias("st"), F.col("st.id") == F.col("s.student_id"), "inner")
    .select(
        "s.*",
        F.col("st.gpa").alias("student_gpa"),
        "st.student_name",
        F.col("st.major").alias("student_major"),
    )
)

with_children = (
    with_details.alias("s")
    .join(df_enrollments.alias("e"), F.col("e.course_id") == F.col("s.course_id"), "inner")
    .select(
        "s.id",
        "s.student_id",
        "s.course_id",
        "s.term",
        "s.grade_status",
        "s.score_adj",
        "s.grade_status_term_label",
        "s.enrollment_term_count",
        "s.enrollment_count_2",
        "s.student_gpa",
        "s.student_name",
        "s.student_major",
        F.col("e.id").alias("enrollment_id"),
        F.col("e.score").alias("enrollment_score"),
        F.col("e.term").alias("enrollment_term"),
    )
)

combined_details_student = (
    with_children.alias("s")
    .join(df_students.alias("st"), F.col("st.id") == F.col("s.student_id"), "inner")
    .join(df_enrollments.alias("e"), F.col("e.id") == F.col("s.enrollment_id"), "inner")
    .select(
        "s.id",
        "s.student_id",
        "s.course_id",
        "s.term",
        "s.grade_status",
        "s.score_adj",
        "s.grade_status_term_label",
        "s.enrollment_term_count",
        "s.enrollment_count_2",
        "s.student_gpa",
        "s.student_name",
        "s.student_major",
        "s.enrollment_id",
        "s.enrollment_score",
        F.col("st.student_name").alias("student_student_name"),
        F.col("st.major").alias("student_major_2"),
        F.col("e.score").alias("enrollment_score_2"),
        F.col("e.term").alias("enrollment_term_2"),
    )
)

with_details_student = (
    combined_details_student.alias("s")
    .join(df_students.alias("st"), F.col("st.id") == F.col("s.student_id"), "inner")
    .join(df_enrollments.alias("e"), F.col("e.id") == F.col("s.enrollment_id"), "left")
    .filter(F.col("st.major") == "Math")
    .select(
        "s.id",
        "s.student_id",
        "s.course_id",
        "s.grade_status",
        "s.grade_status_term_label",
        "s.enrollment_term_count",
        "s.enrollment_count_2",
        "s.student_gpa",
        "s.student_name",
        "s.student_major",
        "s.enrollment_id",
        "s.student_student_name",
        "s.student_major_2",
        "s.enrollment_score_2",
        F.col("st.major").alias("student_major_3"),
        F.col("e.term").alias("enrollment_term"),
        F.col("e.enrolled_at").alias("enrollment_enrolled_at"),
    )
)

result = (
    with_details_student
    .select(
        "id",
        "student_id",
        "grade_status",
        "grade_status_term_label",
        "enrollment_term_count",
        "enrollment_count_2",
        "student_gpa",
        "student_name",
        "enrollment_id",
        "student_student_name",
        "student_major_2",
        "enrollment_score_2",
        "student_major_3",
        F.round(F.col("enrollment_score_2") / F.nullif(F.col("enrollment_term_count"), F.lit(0)), 3).alias("enrollment_score_2_per_enrollment_term_count"),
        F.date_format(F.col("enrollment_enrolled_at"), "yyyy-MM-dd HH:00").alias("enrollment_enrolled_at_label"),
    )
    .orderBy(
        "enrollment_score_2_per_enrollment_term_count",
        "enrollment_enrolled_at_label",
    )
)
