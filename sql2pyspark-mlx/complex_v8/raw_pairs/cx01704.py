from pyspark.sql import functions as F

scoped = (
    df_enrollments
    .filter((F.col("course_id") <= 4) | (F.col("grade_status") != "WITHDRAWN"))
    .select(
        "id",
        "student_id",
        "course_id",
        "term",
        "score",
        "grade_status",
    )
)

derived = (
    scoped
    .withColumn("grade_status_term_label", F.concat_ws(": ", F.col("grade_status"), F.col("term")))
)

with_parent_student = (
    derived.alias("s")
    .join(df_students.alias("st"), F.col("st.id") == F.col("s.student_id"), "inner")
    .select(
        "s.id",
        "s.student_id",
        "s.course_id",
        "s.term",
        "s.score",
        "s.grade_status",
        "s.grade_status_term_label",
        F.col("st.gpa").alias("student_gpa"),
    )
)

screened = (
    with_parent_student.alias("s")
    .join(df_enrollments.alias("e"), (F.col("e.student_id") == F.col("s.student_id")) & (F.col("e.course_id").isNotNull()), "left_anti")
)

annotated = (
    screened
    .select(
        "*",
        F.coalesce(F.col("grade_status"), F.lit("UNKNOWN")).alias("grade_status_or_unknown"),
        F.when(F.col("term").isin("2025T2", "2024T2"), "group_a").when(F.col("term") == "2025T1", "group_b").otherwise("other").alias("term_group"),
    )
)

enrollment_stats = (
    df_enrollments
    .groupBy("course_id")
    .agg(
        F.max("score").alias("max_enrollment_score"),
        F.count("*").alias("enrollment_count"),
    )
)

summarized_rows = (
    annotated.alias("s")
    .join(enrollment_stats.alias("e"), F.col("e.course_id") == F.col("s.course_id"), "left")
    .select(
        "s.*",
        "e.max_enrollment_score",
        F.coalesce(F.col("e.enrollment_count"), F.lit(0)).alias("enrollment_count"),
    )
)

joined = (
    summarized_rows.alias("s")
    .join(df_students.alias("st"), F.col("st.id") == F.col("s.student_id"), "inner")
    .join(df_courses.alias("c"), F.col("c.id") == F.col("s.course_id"), "inner")
    .select(
        "s.id",
        "s.student_id",
        "s.course_id",
        "s.term",
        "s.score",
        "s.grade_status",
        "s.grade_status_term_label",
        "s.student_gpa",
        "s.grade_status_or_unknown",
        "s.term_group",
        "s.max_enrollment_score",
        "s.enrollment_count",
        F.col("st.gpa").alias("student_gpa_2"),
        F.col("st.major").alias("student_major"),
        F.col("c.level").alias("course_level"),
    )
)

result = (
    joined
    .select(
        "id",
        "student_id",
        "course_id",
        "score",
        "grade_status",
        "student_gpa",
        "grade_status_or_unknown",
        "term_group",
        "student_gpa_2",
        "student_major",
        "course_level",
    )
    .orderBy(F.col("student_gpa_2").asc_nulls_last(), F.col("grade_status").desc())
)
