from pyspark.sql import functions as F

valid_students = (
    df_students
    .filter(F.col("cohort").isNotNull())
    .select(
        "id",
        "student_name",
        "cohort",
        "major",
        "enrolled_on",
        "gpa",
        F.substring(F.trim(F.col("student_name")), 1, 3).alias("student_name_prefix"),
        F.concat_ws("-", F.col("student_name"), F.col("major")).alias("student_name_major_label"),
        F.substring(F.trim(F.col("student_name")), 1, 3).alias("student_name_prefix_2"),
    )
)

with_children = (
    valid_students.alias("s")
    .join(df_enrollments.alias("e"), (F.col("e.student_id") == F.col("s.id")) & (F.col("e.term").isin("2024T1", "2025T2", "2024T2")), "left")
    .select(
        "s.id",
        "s.student_name",
        "s.cohort",
        "s.gpa",
        "s.student_name_major_label",
        F.col("e.id").alias("enrollment_id"),
        F.col("e.term").alias("enrollment_term"),
        F.col("e.grade_status").alias("enrollment_grade_status"),
    )
)

cohort_stats = (
    with_children
    .groupBy("cohort")
    .agg(F.percentile_approx("gpa", 0.5).alias("median_gpa"))
)

vs_group = (
    with_children.alias("s")
    .join(cohort_stats.alias("g"), F.col("g.cohort") == F.col("s.cohort"), "inner")
    .filter(F.col("s.gpa") >= F.col("g.median_gpa"))
    .select(
        "s.*",
        "g.median_gpa",
        F.round(F.col("s.gpa") - F.col("g.median_gpa"), 2).alias("gpa_minus_median"),
    )
)

result = (
    vs_group
    .select(
        "id",
        "student_name",
        "cohort",
        "gpa",
        "enrollment_term",
        "enrollment_grade_status",
        "gpa_minus_median",
        F.coalesce(F.col("gpa_minus_median"), F.lit(0.0)).alias("gpa_minus_median_filled"),
    )
    .orderBy("enrollment_term", F.col("student_name").desc())
)
