from pyspark.sql import functions as F

valid = (
    df_students
    .filter(~F.col("major").isin("Bio", "Biz"))
    .select(
        "id",
        "cohort",
        "enrolled_on",
        "gpa",
        F.concat_ws("-", F.col("major"), F.col("cohort")).alias("major_cohort_label"),
        F.lower(F.regexp_replace(F.trim(F.col("major")), " ", "_")).alias("major_slug"),
    )
)

derived = (
    valid
    .filter(F.col("enrolled_on") < F.lit("2025-05-07").cast("date"))
    .withColumn("major_cohort_label_ref", F.concat(F.col("major_cohort_label"), F.lit("#"), F.col("id").cast("string")))
)

summary = (
    derived
    .filter(F.col("gpa") < 3.3)
    .groupBy(
        F.date_trunc("month", "enrolled_on").alias("enrolled_on_month"),
        "cohort",
    )
    .agg(
        F.max("enrolled_on").alias("last_enrolled_on"),
        F.countDistinct("cohort").alias("distinct_cohorts"),
        F.percentile_approx("gpa", 0.5).alias("median_gpa"),
        F.max("gpa").alias("max_gpa_check"),
    )
    .filter(F.col("max_gpa_check") > 2.1)
    .drop("max_gpa_check")
)

result = (
    summary
    .groupBy("enrolled_on_month", "distinct_cohorts")
    .pivot("cohort", ["C2022", "C2024", "C2023", "C2021"])
    .agg(F.min("median_gpa"))
    .orderBy("enrolled_on_month", "distinct_cohorts")
)
