from pyspark.sql import functions as F

valid_students = (
    df_students
    .filter(F.col("gpa").between(1.7, 3.4))
    .select(
        "id",
        "student_name",
        "cohort",
        "enrolled_on",
        "gpa",
        "is_international",
    )
)

student_side = (
    valid_students
    .filter((F.col("cohort").isin("C2021", "C2022")) & (F.upper(F.trim(F.col("student_name"))).like("L%")))
    .select(F.col("cohort").alias("name"), F.col("enrolled_on").alias("day"), "id")
)

student_side_2 = (
    df_students
    .select(F.col("major").alias("name"), F.col("enrolled_on").alias("day"), "id")
)

all_events = student_side.unionByName(student_side_2)

result = (
    all_events
    .groupBy("name")
    .agg(
        F.count("*").alias("n_rows"),
        F.sum("id").alias("total_id"),
        F.max("day").alias("latest_day"),
    )
    .orderBy("name")
)
