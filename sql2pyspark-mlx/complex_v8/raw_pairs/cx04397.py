from pyspark.sql import functions as F

valid = (
    df_students
    .filter(F.col("cohort") == "C2023")
    .select(
        "id",
        "student_name",
        "cohort",
        "major",
        "enrolled_on",
        "gpa",
        "is_international",
        F.lower(F.regexp_replace(F.trim(F.col("student_name")), " ", "_")).alias("student_name_slug"),
        F.concat_ws("-", F.col("student_name"), F.col("major")).alias("student_name_major_label"),
        F.trunc(F.col("enrolled_on"), "MM").alias("enrolled_on_month_start"),
    )
)

detail_rows = (
    valid.alias("s")
    .join(df_enrollments.alias("e"), F.col("e.student_id") == F.col("s.id"), "left")
    .select(
        "s.id",
        "s.cohort",
        "s.enrolled_on",
        "s.gpa",
        "s.is_international",
        "s.student_name_slug",
        F.col("e.id").alias("enrollment_id"),
        F.col("e.term").alias("enrollment_term"),
        F.col("e.score").alias("enrollment_score"),
        F.col("e.enrolled_at").alias("enrollment_enrolled_at"),
    )
)

result = (
    detail_rows
    .rollup("enrollment_term", "enrollment_id")
    .agg(
        F.count("*").alias("row_count"),
        F.round(F.sum("enrollment_score") / F.nullif(F.sum("gpa"), F.lit(0)), 3).alias("enrollment_score_to_gpa_ratio"),
        F.count(F.when(F.col("cohort") != "C2022", 1)).alias("n_cohort_c2022"),
    )
    .orderBy(
        F.col("enrollment_term").asc_nulls_last(),
        F.col("enrollment_id").asc_nulls_last(),
    )
)
