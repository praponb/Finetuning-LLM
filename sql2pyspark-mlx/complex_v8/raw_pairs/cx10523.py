from pyspark.sql import functions as F

base = (
    df_students
    .filter((F.length(F.trim(F.col("student_name"))) >= 7) & (F.col("enrolled_on") < F.lit("2025-05-07").cast("date")))
    .select(
        "id",
        "student_name",
        "cohort",
        "gpa",
        "is_international",
        F.when(F.col("major").isin("Bio", "CS"), "group_a").when(F.col("major") == "Math", "group_b").otherwise("other").alias("major_group"),
        F.date_trunc("quarter", F.col("enrolled_on")).alias("enrolled_on_quarter"),
        F.when(F.col("is_international"), "yes").otherwise("no").alias("is_international_label"),
    )
)

detail_rows_enrollments = (
    base.alias("s")
    .join(df_enrollments.alias("e"), F.col("e.student_id") == F.col("s.id"), "inner")
    .select(
        "s.id",
        "s.student_name",
        "s.gpa",
        "s.is_international",
        "s.major_group",
        "s.is_international_label",
        F.col("e.id").alias("enrollment_id"),
        F.col("e.enrolled_at").alias("enrollment_enrolled_at"),
        F.col("e.score").alias("enrollment_score"),
        F.col("e.term").alias("enrollment_term"),
        F.col("e.course_id").alias("enrollment_course_id"),
    )
)

combined_details = (
    detail_rows_enrollments.alias("s")
    .join(df_enrollments.alias("e"), F.col("e.id") == F.col("s.enrollment_id"), "inner")
    .join(df_students.alias("st"), F.col("st.id") == F.col("e.student_id"), "left")
    .select(
        "s.id",
        "s.gpa",
        "s.major_group",
        "s.enrollment_id",
        "s.enrollment_score",
        "s.enrollment_course_id",
        F.col("e.score").alias("enrollment_score_2"),
        F.col("e.enrolled_at").alias("enrollment_enrolled_at_2"),
        F.col("st.is_international").alias("student_is_international"),
    )
)

result = (
    combined_details
    .groupBy("student_is_international", "enrollment_enrolled_at_2")
    .pivot("major_group", ["group_b", "group_a", "other"])
    .agg(F.min("gpa").alias("min_v"), F.count("gpa").alias("n"))
)
