from pyspark.sql import functions as F

filtered_enrollments = (
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
        F.date_trunc("day", F.col("enrolled_at")).alias("enrolled_at_day"),
        F.concat_ws(": ", F.col("term"), F.col("grade_status")).alias("term_grade_status_label"),
        F.date_trunc("month", F.col("enrolled_at")).alias("enrolled_at_month"),
    )
)

enrollment_stats = (
    df_enrollments
    .groupBy("student_id")
    .agg(F.max("enrolled_at").alias("last_enrollment_enrolled_at"))
)

with_stats = (
    filtered_enrollments.alias("s")
    .join(enrollment_stats.alias("e"), F.col("e.student_id") == F.col("s.student_id"), "left")
    .select("s.*", "e.last_enrollment_enrolled_at")
)

result = (
    with_stats
    .select(
        "id",
        "student_id",
        "course_id",
        "term",
        "score",
        "enrolled_at",
        "term_grade_status_label",
        "enrolled_at_month",
        "last_enrollment_enrolled_at",
        F.round((F.unix_timestamp(F.col("enrolled_at_month")) - F.unix_timestamp(F.col("enrolled_at"))) / 3600, 1).alias("hours_enrolled_at_to_enrolled_at_month"),
        F.concat(F.col("grade_status"), F.lit("#"), F.col("course_id").cast("string")).alias("grade_status_ref"),
    )
    .orderBy(F.col("grade_status_ref").desc(), "last_enrollment_enrolled_at")
)
