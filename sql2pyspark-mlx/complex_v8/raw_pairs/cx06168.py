from pyspark.sql import functions as F

valid = (
    df_students
    .filter(F.col("enrolled_on") < F.lit("2025-03-15").cast("date"))
    .select(
        "id",
        "student_name",
        "cohort",
        "major",
        "enrolled_on",
        "gpa",
        "is_international",
        F.date_add(F.col("enrolled_on"), 7).alias("enrolled_on_plus_7d"),
    )
)

student_rows = (
    valid
    .filter(F.col("gpa") > 2.5)
    .select("cohort", "enrolled_on")
)

student_side = df_students.select("major", "enrolled_on")

student_side_2 = (
    df_students
    .filter(F.col("enrolled_on") < F.lit("2025-07-18").cast("date"))
    .select("cohort", "enrolled_on")
)

unioned = (
    student_rows
    .union(student_side)
    .union(student_side_2)
)

result = (
    unioned
    .groupBy("cohort")
    .agg(
        F.count("*").alias("n_rows"),
        F.max("enrolled_on").alias("latest_enrolled_on"),
    )
    .orderBy("cohort")
)
