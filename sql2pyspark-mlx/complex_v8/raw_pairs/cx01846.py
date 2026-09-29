from pyspark.sql import functions as F

cleaned_students = (
    df_students
    .filter((F.col("major") == "Art") & (F.col("enrolled_on").isNotNull()))
    .select(
        "id",
        "student_name",
        "enrolled_on",
        "gpa",
        "is_international",
        F.round(F.col("gpa") * 0.9, 2).alias("gpa_adj"),
        F.date_add(F.col("enrolled_on"), 30).alias("enrolled_on_plus_30d"),
    )
)

qualified = (
    cleaned_students
    .crossJoin(cleaned_students.agg(F.avg("gpa_adj").alias("overall_avg_gpa_adj")))
    .filter(F.col("gpa_adj") > F.col("overall_avg_gpa_adj"))
    .drop("overall_avg_gpa_adj")
)

result = (
    qualified
    .filter(F.col("is_international"))
    .select(
        "id",
        "student_name",
        "enrolled_on",
        "gpa",
        "is_international",
        "gpa_adj",
        "enrolled_on_plus_30d",
    )
    .orderBy(F.col("enrolled_on_plus_30d").asc(), "gpa_adj")
)
