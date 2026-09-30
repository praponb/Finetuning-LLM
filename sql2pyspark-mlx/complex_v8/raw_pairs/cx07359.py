from pyspark.sql import functions as F

base = (
    df_enrollments
    .filter((F.col("grade_status").isin("WITHDRAWN", "FAIL")) & (F.col("course_id").isNotNull()))
    .select(
        "id",
        "student_id",
        "course_id",
        "term",
        "score",
        "grade_status",
        F.concat_ws("-", F.col("grade_status"), F.col("term")).alias("grade_status_term_label"),
    )
)

enrollment_side = (
    df_enrollments
    .filter((F.col("term") == "2025T2") & (F.to_date(F.col("enrolled_at")) < F.lit("2025-02-24").cast("date")))
    .select(
        F.lit("enrollment").alias("origin"),
        "student_id",
        "enrolled_at",
        "grade_status",
    )
)

enrollment_side_2 = (
    df_enrollments
    .select(
        F.lit("enrollment_2").alias("origin"),
        "student_id",
        "enrolled_at",
        "grade_status",
    )
)

all_events = enrollment_side.unionByName(enrollment_side_2)

result = (
    all_events
    .groupBy("origin")
    .agg(
        F.count("*").alias("n_rows"),
        F.sum("student_id").alias("total_student_id"),
        F.max("enrolled_at").alias("latest_enrolled_at"),
    )
    .orderBy("origin")
)
