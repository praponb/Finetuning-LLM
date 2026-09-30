from pyspark.sql import functions as F

base = (
    df_enrollments
    .filter((F.col("term") == "2024T1") & (F.col("student_id").isNotNull()))
    .select(
        "id",
        "student_id",
        "course_id",
        "term",
        "grade_status",
        "enrolled_at",
        F.date_trunc("quarter", F.col("enrolled_at")).alias("enrolled_at_quarter"),
        F.when(F.col("term").isin("2024T2", "2025T2"), "group_a").when(F.col("term") == "2024T1", "group_b").otherwise("other").alias("term_group"),
        F.lower(F.trim(F.col("grade_status"))).alias("grade_status_clean"),
    )
)

tagged = (
    base
    .filter(F.col("grade_status") != "INCOMPLETE")
    .withColumn("days_since_enrolled_at", F.datediff(F.lit("2026-01-01").cast("date"), F.col("enrolled_at")))
)

enriched = (
    tagged
    .select(
        "id",
        "student_id",
        "course_id",
        "term",
        "grade_status",
        "enrolled_at_quarter",
        "grade_status_clean",
        F.coalesce(F.col("days_since_enrolled_at"), F.lit(0)).alias("days_since_enrolled_at_filled"),
        F.length(F.trim(F.col("term"))).alias("term_len"),
    )
)

result = (
    enriched
    .filter(F.col("grade_status") != "PASS")
    .select(
        "id",
        "course_id",
        "term",
        "enrolled_at_quarter",
        "grade_status_clean",
        "days_since_enrolled_at_filled",
        "term_len",
    )
    .orderBy("term_len")
)
