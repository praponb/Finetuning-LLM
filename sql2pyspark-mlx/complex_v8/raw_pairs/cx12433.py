from pyspark.sql import functions as F

filtered_students = df_students

enriched = (
    filtered_students
    .select(
        "id",
        "cohort",
        "major",
        "enrolled_on",
        "gpa",
        "is_international",
        F.lower(F.regexp_replace(F.trim(F.col("major")), " ", "_")).alias("major_slug"),
        F.month(F.col("enrolled_on")).alias("enrolled_on_month"),
        F.when(F.col("gpa") >= 3.2, "large").when(F.col("gpa") >= 2.7, "medium").otherwise("small").alias("gpa_tier"),
    )
)

screened = (
    enriched.alias("s")
    .join(df_enrollments.alias("e"), F.col("e.student_id") == F.col("s.id"), "left_semi")
)

result = (
    screened
    .groupBy("enrolled_on_month", "enrolled_on")
    .pivot("cohort", ["C2024", "C2023", "C2021"])
    .agg(F.sum("gpa"))
)
