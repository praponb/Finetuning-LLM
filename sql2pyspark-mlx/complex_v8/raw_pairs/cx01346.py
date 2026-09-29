from pyspark.sql import functions as F

recent_enrollments = (
    df_enrollments
    .filter((F.col("course_id").isNotNull()) & (F.col("term").isNotNull()))
    .select(
        "id",
        "student_id",
        "course_id",
        "term",
        "grade_status",
        "enrolled_at",
        F.lower(F.regexp_replace(F.trim(F.col("grade_status")), " ", "_")).alias("grade_status_slug"),
    )
)

qualified = (
    recent_enrollments.alias("s")
    .join(df_enrollments.alias("e"), (F.col("e.course_id") == F.col("s.course_id")) & (F.col("e.grade_status").isNotNull()), "left_anti")
    .filter(F.col("enrolled_at") < F.lit("2025-03-08 09:00:00").cast("timestamp"))
)

result = (
    qualified
    .filter(~F.col("grade_status").isin("PASS", "INCOMPLETE"))
    .select(
        "id",
        "student_id",
        "course_id",
        "term",
        "grade_status",
        "enrolled_at",
        "grade_status_slug",
        F.initcap(F.trim(F.col("grade_status_slug"))).alias("grade_status_slug_clean"),
    )
)
