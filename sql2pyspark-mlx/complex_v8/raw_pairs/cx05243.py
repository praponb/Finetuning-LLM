from pyspark.sql import functions as F

prepared_enrollments = (
    df_enrollments
    .filter((F.col("grade_status").isin("PASS", "INCOMPLETE", "WITHDRAWN")) & (F.col("score") >= 38.3))
    .select(
        "id",
        "student_id",
        "course_id",
        "term",
        "score",
        "grade_status",
        "enrolled_at",
        F.coalesce(F.col("term"), F.lit("UNKNOWN")).alias("term_or_unknown"),
    )
)

derived = (
    prepared_enrollments
    .filter(F.col("score") <= 79.3)
    .select(
        "*",
        F.concat(F.col("term"), F.lit("#"), F.col("course_id").cast("string")).alias("term_ref"),
        F.date_trunc("quarter", F.col("enrolled_at")).alias("enrolled_at_quarter"),
        F.date_format(F.col("enrolled_at"), "MMM yyyy").alias("enrolled_at_label"),
    )
)

enrollment_stats = (
    df_enrollments
    .groupBy("student_id")
    .agg(F.max("enrolled_at").alias("last_enrollment_enrolled_at"))
)

profiled = (
    derived.alias("s")
    .join(enrollment_stats.alias("e"), F.col("e.student_id") == F.col("s.student_id"), "left")
    .select("s.*", "e.last_enrollment_enrolled_at")
)

result = (
    profiled
    .cube("term_or_unknown", "grade_status", "course_id")
    .agg(
        F.sum(F.when(F.col("term") != "2025T2", F.col("score")).otherwise(0)).alias("score_term_2025t2"),
    )
)
