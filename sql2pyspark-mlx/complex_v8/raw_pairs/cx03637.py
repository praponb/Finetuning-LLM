from pyspark.sql import functions as F

prepared_enrollments = (
    df_enrollments
    .select(
        "id",
        "student_id",
        "course_id",
        "term",
        "score",
        "grade_status",
        "enrolled_at",
        F.to_date(F.col("enrolled_at")).alias("enrolled_at_date"),
    )
)

screened = (
    prepared_enrollments.alias("s")
    .join(df_enrollments.alias("e"), (F.col("e.student_id") == F.col("s.student_id")) & (F.to_date(F.col("e.enrolled_at")) < F.lit("2025-03-08").cast("date")) & (F.col("e.grade_status") == "PASS"), "left_anti")
)

result = (
    screened
    .groupBy("enrolled_at", "student_id")
    .pivot("grade_status", ["WITHDRAWN", "INCOMPLETE", "PASS", "FAIL"])
    .agg(F.sum("score"))
)
