from pyspark.sql import functions as F

covered = (
    df_enrollments.alias("a")
    .join(
        df_enrollments.alias("b"),
        (F.col("b.student_id") == F.col("a.student_id"))
        & (F.col("b.enrolled_at") < F.col("a.enrolled_at"))
        & (F.col("b.enrolled_at") >= F.col("a.enrolled_at") - F.expr("INTERVAL 1 DAYS")),
        "left",
    )
    .groupBy("a.id", "a.student_id", "a.enrolled_at")
    .agg(
        F.count("b.id").alias("prior_rows_1d"),
        F.sum("b.score").alias("prior_score_1d"),
    )
)

enriched = (
    covered.alias("s")
    .join(df_students.alias("st"), F.col("st.id") == F.col("s.student_id"), "left")
    .select("s.*", "st.student_name", F.col("st.major").alias("student_major"))
)

with_parent_student = (
    enriched.alias("s")
    .join(df_students.alias("st"), F.col("st.id") == F.col("s.student_id"), "left")
    .select(
        "s.id",
        "s.student_id",
        "s.enrolled_at",
        "s.prior_rows_1d",
        "s.student_name",
        F.col("st.student_name").alias("student_student_name"),
    )
)

exploded_detail = (
    with_parent_student.alias("s")
    .join(df_enrollments.alias("e"), F.col("e.student_id") == F.col("s.student_id"), "left")
    .select(
        "s.id",
        "s.student_id",
        "s.student_name",
        "s.student_student_name",
        F.col("e.id").alias("enrollment_id"),
        F.col("e.term").alias("enrollment_term"),
        F.col("e.course_id").alias("enrollment_course_id"),
    )
)

result = (
    exploded_detail
    .filter(F.col("student_id") <= 17)
    .select(
        "id",
        "student_id",
        "student_name",
        "enrollment_term",
        "enrollment_course_id",
        F.concat(F.col("enrollment_term"), F.lit("#"), F.col("enrollment_id").cast("string")).alias("enrollment_term_ref"),
    )
    .orderBy(
        F.col("enrollment_term").desc_nulls_first(),
        F.col("student_name").desc_nulls_first(),
    )
)
