from pyspark.sql import functions as F

cleaned = (
    df_enrollments
    .filter((F.col("score") <= 77.0) & (F.col("enrolled_at") < F.lit("2025-03-08 08:00:00").cast("timestamp")))
    .select(
        "id",
        "student_id",
        "course_id",
        "term",
        "grade_status",
        "enrolled_at",
    )
)

enrollment_stats = (
    df_enrollments
    .groupBy("course_id")
    .agg(F.max("enrolled_at").alias("last_enrollment_enrolled_at"))
)

with_counts = (
    cleaned.alias("s")
    .join(enrollment_stats.alias("e"), F.col("e.course_id") == F.col("s.course_id"), "left")
    .select("s.*", "e.last_enrollment_enrolled_at")
)

combined_details_student = (
    with_counts.alias("s")
    .join(df_students.alias("st"), F.col("st.id") == F.col("s.student_id"), "inner")
    .filter(~F.col("st.major").isin("Bio", "Biz"))
    .select(
        "s.id",
        "s.student_id",
        "s.course_id",
        "s.term",
        "s.grade_status",
        F.col("st.major").alias("student_major"),
        F.col("st.is_international").alias("student_is_international"),
        "st.student_name",
    )
)

tagged = (
    combined_details_student
    .filter(~F.col("grade_status").isin("PASS", "WITHDRAWN"))
    .withColumn("term_len", F.length(F.trim(F.col("term"))))
)

result = (
    tagged
    .select(
        "id",
        "student_id",
        "course_id",
        "term",
        "grade_status",
        "student_major",
        "student_is_international",
        "student_name",
        "term_len",
        F.abs(F.col("term_len") - 8).alias("term_len_distance"),
        F.when(F.col("student_is_international"), 1).otherwise(0).alias("student_is_international_flag"),
    )
    .orderBy("grade_status", F.col("student_id").desc())
)
