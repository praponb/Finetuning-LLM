from pyspark.sql import functions as F

filtered = (
    df_students
    .filter(F.length(F.trim(F.col("student_name"))) >= 5)
    .select(
        "id",
        "student_name",
        "cohort",
        "major",
        "enrolled_on",
        "gpa",
        "is_international",
    )
)

without_matches = (
    filtered
    .crossJoin(filtered.agg(F.avg("gpa").alias("overall_avg_gpa")))
    .filter(F.col("gpa") > F.col("overall_avg_gpa"))
    .drop("overall_avg_gpa")
)

per_group = (
    without_matches
    .filter(F.col("cohort") == "C2023")
    .groupBy("major")
    .agg(
        F.count(F.when(F.col("cohort").isin("C2022", "C2024"), 1)).alias("n_cohort_in_c2022_c2024"),
        F.sum(F.when(F.col("is_international"), 1).otherwise(0)).alias("n_is_international_true"),
        F.round(100 * F.count(F.when(F.col("cohort") == "C2021", 1)) / F.count("*"), 1).alias("pct_cohort_c2021"),
        F.max("gpa").alias("max_gpa_check"),
    )
    .filter(F.col("max_gpa_check") > 2.1)
    .drop("max_gpa_check")
)

result = (
    per_group
    .select(
        "major",
        "n_is_international_true",
        "pct_cohort_c2021",
        F.upper(F.trim(F.col("major"))).alias("major_clean"),
        (F.floor(F.col("n_cohort_in_c2022_c2024") / 2) * 2).alias("n_cohort_in_c2022_c2024_bucket"),
    )
    .orderBy(F.col("major").asc(), F.col("major_clean").desc_nulls_first())
    .limit(20)
)
