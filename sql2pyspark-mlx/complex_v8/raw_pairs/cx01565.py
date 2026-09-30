from pyspark.sql import functions as F

valid_students = (
    df_students
    .select(
        "id",
        "student_name",
        "cohort",
        "enrolled_on",
        "is_international",
    )
)

screened = (
    valid_students.alias("s")
    .join(df_enrollments.alias("e"), (F.col("e.student_id") == F.col("s.id")) & (F.col("e.term") == "2024T2"), "left_semi")
)

enrollment_stats = (
    df_enrollments
    .groupBy("student_id")
    .agg(F.max("score").alias("max_enrollment_score"))
)

profiled = (
    screened.alias("s")
    .join(enrollment_stats.alias("e"), F.col("e.student_id") == F.col("s.id"), "left")
    .select("s.*", "e.max_enrollment_score")
)

result = (
    profiled
    .groupBy("enrolled_on", "is_international")
    .pivot("cohort", ["C2024", "C2022", "C2023"])
    .agg(F.sum("max_enrollment_score"))
)
