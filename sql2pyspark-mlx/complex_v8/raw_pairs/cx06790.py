from pyspark.sql import functions as F

prepared = (
    df_students
    .filter(F.col("enrolled_on").between(F.lit("2024-03-15").cast("date"), F.lit("2025-07-03").cast("date")))
    .select(
        "id",
        "cohort",
        "enrolled_on",
        "gpa",
        "is_international",
        F.concat_ws(": ", F.col("cohort"), F.col("major")).alias("cohort_major_label"),
    )
)

stats = (
    prepared
    .groupBy(F.date_trunc("month", "enrolled_on").alias("enrolled_on_month"))
    .agg(
        F.max("enrolled_on").alias("last_enrolled_on"),
        F.sum("gpa").alias("total_gpa"),
        F.min("enrolled_on").alias("first_enrolled_on"),
    )
)

tagged = stats.withColumn("enrolled_on_month_year", F.date_trunc("year", F.col("enrolled_on_month")))

without_matches = (
    tagged
    .crossJoin(tagged.agg(F.min("total_gpa").alias("overall_min_total_gpa")))
    .filter(F.col("total_gpa") > F.col("overall_min_total_gpa"))
    .drop("overall_min_total_gpa")
)

result = (
    without_matches
    .select(
        "enrolled_on_month",
        "last_enrolled_on",
        "total_gpa",
        "first_enrolled_on",
        F.coalesce(F.col("total_gpa"), F.lit(0.0)).alias("total_gpa_filled"),
    )
    .orderBy(
        F.col("enrolled_on_month").desc(),
        F.col("first_enrolled_on").desc_nulls_first(),
    )
    .limit(5)
)
