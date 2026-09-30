from pyspark.sql import functions as F

valid_students = (
    df_students
    .select(
        "id",
        "student_name",
        "cohort",
        "enrolled_on",
        "gpa",
        "is_international",
        F.date_format(F.col("enrolled_on"), "MMM yyyy").alias("enrolled_on_label"),
        F.date_format(F.col("enrolled_on"), "yyyy-MM").alias("enrolled_on_label_2"),
        F.datediff(F.lit("2025-12-31").cast("date"), F.col("enrolled_on")).alias("days_since_enrolled_on"),
    )
)

labeled = (
    valid_students
    .filter(~F.col("cohort").isin("C2024", "C2023"))
    .select(
        "id",
        "student_name",
        "enrolled_on",
        "is_international",
        "enrolled_on_label",
        "enrolled_on_label_2",
        "days_since_enrolled_on",
        F.least(F.col("gpa"), F.col("days_since_enrolled_on")).alias("least_gpa_days_since_enrolled_on"),
        F.least(F.col("days_since_enrolled_on"), F.col("gpa")).alias("least_days_since_enrolled_on_gpa"),
    )
)

per_group = (
    labeled
    .filter(F.col("least_gpa_days_since_enrolled_on") > 70.0)
    .groupBy(
        "is_international",
        F.date_trunc("month", "enrolled_on").alias("enrolled_on_month"),
    )
    .agg(
        F.sum("least_gpa_days_since_enrolled_on").alias("total_least_gpa_days_since_enrolled_on"),
        F.array_sort(F.collect_set("student_name")).alias("student_name_list"),
        F.min("least_days_since_enrolled_on_gpa").alias("min_least_days_since_enrolled_on_gpa"),
        F.count(F.when(~F.col("is_international"), 1)).alias("n_is_international_false"),
        F.round(F.avg("days_since_enrolled_on"), 2).alias("avg_days_since_enrolled_on"),
    )
)

result = (
    per_group
    .select(
        "is_international",
        "enrolled_on_month",
        "total_least_gpa_days_since_enrolled_on",
        "student_name_list",
        "min_least_days_since_enrolled_on_gpa",
        "n_is_international_false",
        "avg_days_since_enrolled_on",
        F.abs(F.col("n_is_international_false") - 6).alias("n_is_international_false_distance"),
    )
    .orderBy(
        F.col("avg_days_since_enrolled_on").desc_nulls_first(),
        F.col("n_is_international_false").desc(),
        "is_international",
        "enrolled_on_month",
    )
    .limit(10)
)
