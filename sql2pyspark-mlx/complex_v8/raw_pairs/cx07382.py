from pyspark.sql import functions as F

filtered = (
    df_students
    .filter(F.col("student_name").isNotNull())
    .select(
        "id",
        "student_name",
        "cohort",
        "enrolled_on",
        "gpa",
        "is_international",
        F.when(F.col("gpa") >= 3.3, "large").when(F.col("gpa") >= 2.5, "medium").otherwise("small").alias("gpa_tier"),
    )
)

student_side = (
    filtered
    .select(
        F.col("id").alias("qty"),
        F.col("enrolled_on").alias("day"),
        F.col("gpa").alias("amount"),
        F.lit("student").alias("src"),
    )
)

student_side_2 = (
    df_students
    .select(
        F.col("id").alias("qty"),
        F.col("enrolled_on").alias("day"),
        F.col("gpa").alias("amount"),
        F.lit("student_2").alias("src"),
    )
)

student_side_3 = (
    df_students
    .filter(F.col("major").isNotNull())
    .select(
        F.col("id").alias("qty"),
        F.col("enrolled_on").alias("day"),
        F.col("gpa").alias("amount"),
        F.lit("student_3").alias("src"),
    )
)

unioned = (
    student_side
    .union(student_side_2)
    .union(student_side_3)
)

result = (
    unioned
    .groupBy("src")
    .agg(
        F.count("*").alias("n_rows"),
        F.sum("qty").alias("total_qty"),
        F.max("day").alias("latest_day"),
    )
    .orderBy("src")
)
