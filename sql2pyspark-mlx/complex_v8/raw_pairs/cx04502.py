from pyspark.sql import functions as F

student_rows = (
    df_students
    .filter((~F.col("is_international")) & (F.length(F.trim(F.col("student_name"))) >= 6))
    .select("major", "enrolled_on")
)

student_rows_2 = df_students.select("student_name", "enrolled_on")

combined = (
    student_rows
    .union(student_rows_2)
    .distinct()
)

result = (
    combined
    .groupBy("major")
    .agg(
        F.count("*").alias("n_rows"),
        F.max("enrolled_on").alias("latest_enrolled_on"),
    )
    .orderBy("major")
)
