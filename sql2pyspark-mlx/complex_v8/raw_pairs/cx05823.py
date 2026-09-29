from pyspark.sql import functions as F

recent_enrollments = (
    df_enrollments
    .filter(F.to_date(F.col("enrolled_at")) < F.lit("2025-02-24").cast("date"))
    .select(
        "id",
        "student_id",
        "course_id",
        "term",
        "score",
        "grade_status",
        "enrolled_at",
        F.initcap(F.trim(F.col("grade_status"))).alias("grade_status_clean"),
    )
)

labeled = (
    recent_enrollments
    .select(
        "id",
        "course_id",
        "term",
        "score",
        "grade_status",
        "grade_status_clean",
        F.concat(F.col("term"), F.lit("#"), F.col("course_id").cast("string")).alias("term_ref"),
        F.datediff(F.lit("2025-12-31").cast("date"), F.col("enrolled_at")).alias("days_since_enrolled_at"),
        F.date_format(F.col("enrolled_at"), "MMM yyyy").alias("enrolled_at_label"),
    )
)

result = (
    labeled
    .filter(F.col("days_since_enrolled_at") >= 105)
    .select(
        "id",
        "course_id",
        "term",
        "score",
        "days_since_enrolled_at",
        F.upper(F.trim(F.col("enrolled_at_label"))).alias("enrolled_at_label_clean"),
    )
    .orderBy(
        F.col("days_since_enrolled_at").desc_nulls_first(),
        F.col("id").desc_nulls_first(),
    )
)
