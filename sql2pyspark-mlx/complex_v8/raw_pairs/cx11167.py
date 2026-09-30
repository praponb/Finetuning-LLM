from pyspark.sql import functions as F

scoped_students = (
    df_students
    .filter((F.col("is_international")) & (~F.col("major").isin("Bio", "Math")))
    .select(
        "id",
        "student_name",
        "cohort",
        "major",
    )
)

student_rows = (
    df_students
    .filter((~F.col("is_international")) & (F.col("enrolled_on") < F.lit("2025-03-20").cast("date")))
    .select("id", "enrolled_on", F.lit("student").alias("record_type"))
)

student_rows_2 = df_students.select("id", "enrolled_on", F.lit("student_2").alias("record_type"))

all_events = (
    student_rows
    .union(student_rows_2)
    .distinct()
)

result = (
    all_events
    .groupBy("record_type")
    .agg(
        F.count("*").alias("n_rows"),
        F.sum("id").alias("total_id"),
        F.max("enrolled_on").alias("latest_enrolled_on"),
    )
    .orderBy("record_type")
)
