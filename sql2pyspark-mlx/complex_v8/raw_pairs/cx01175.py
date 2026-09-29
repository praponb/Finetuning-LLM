from pyspark.sql import functions as F

student_rows = (
    df_students
    .filter((F.col("enrolled_on") >= F.lit("2024-04-05").cast("date")) & (F.upper(F.trim(F.col("student_name"))).like("B%")))
    .select("id", "enrolled_on")
)

student_rows_2 = (
    df_students
    .filter((F.length(F.trim(F.col("student_name"))) >= 5) & (~F.col("is_international")))
    .select("id", "enrolled_on")
)

stacked = (
    student_rows
    .union(student_rows_2)
    .distinct()
)

result = (
    stacked
    .groupBy("id")
    .agg(
        F.count("*").alias("n_rows"),
        F.max("enrolled_on").alias("latest_enrolled_on"),
    )
    .orderBy("id")
)
