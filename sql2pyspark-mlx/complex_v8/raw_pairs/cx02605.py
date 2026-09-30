from pyspark.sql import functions as F

student_rows = (
    df_students
    .filter((F.lower(F.col("student_name")).like("%er%")) & (F.col("major").isin("Math", "CS")))
    .select(
        "cohort",
        F.col("student_name").alias("category"),
        F.col("enrolled_on").alias("event_date"),
        F.lit("student").alias("src"),
    )
)

student_side = (
    df_students
    .filter(F.upper(F.trim(F.col("student_name"))).like("I%"))
    .select(
        "major",
        "student_name",
        "enrolled_on",
        F.lit("student_2").alias("src"),
    )
)

unioned = (
    student_rows
    .union(student_side)
    .distinct()
)

result = (
    unioned
    .groupBy("src")
    .agg(
        F.count("*").alias("n_rows"),
        F.max("event_date").alias("latest_event_date"),
    )
    .orderBy("src")
)
