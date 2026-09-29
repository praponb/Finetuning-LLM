from pyspark.sql import functions as F

prepared_students = (
    df_students
    .filter((F.year(F.col("enrolled_on")) == 2025) & (F.lower(F.col("student_name")).like("%s %")))
    .select(
        "id",
        "student_name",
        "cohort",
        "major",
        "enrolled_on",
        "gpa",
        "is_international",
        F.quarter(F.col("enrolled_on")).alias("enrolled_on_quarter"),
    )
)

enrollment_stats = (
    df_enrollments
    .groupBy("student_id")
    .agg(F.max("enrolled_at").alias("last_enrollment_enrolled_at"))
)

with_counts = (
    prepared_students.alias("s")
    .join(enrollment_stats.alias("e"), F.col("e.student_id") == F.col("s.id"), "left")
    .select("s.*", "e.last_enrollment_enrolled_at")
)

tagged = (
    with_counts
    .select(
        "*",
        F.add_months(F.col("enrolled_on"), 12).alias("enrolled_on_plus_12m"),
    )
)

enriched = (
    tagged
    .filter(F.col("major") == "Bio")
    .select(
        "*",
        (F.col("gpa") - F.col("enrolled_on_quarter")).alias("gpa_minus_enrolled_on_quarter"),
    )
)

result = (
    enriched
    .groupingSets(
        [["cohort", "enrolled_on_quarter"], ["cohort"], []],
        "cohort", "enrolled_on_quarter",
    )
    .agg(F.round(F.sum("gpa"), 2).alias("total_gpa"))
    .orderBy(
        F.col("cohort").asc_nulls_last(),
        F.col("enrolled_on_quarter").asc_nulls_last(),
    )
)
