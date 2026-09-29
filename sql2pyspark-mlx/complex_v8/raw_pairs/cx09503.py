from pyspark.sql import functions as F

student_side = (
    df_students
    .filter((F.col("cohort").isin("C2021", "C2024", "C2023")) & (F.col("gpa") <= 3.3))
    .select("id", F.col("cohort").alias("category"))
)

course_side = (
    df_courses
    .filter((F.upper(F.trim(F.col("course_code"))).like("C%")) & (F.col("dept") == "Art"))
    .select("credits", "dept")
)

all_events = student_side.union(course_side)

result = (
    all_events
    .groupBy("category")
    .agg(F.count("*").alias("n_rows"), F.sum("id").alias("total_id"))
    .orderBy("category")
)
