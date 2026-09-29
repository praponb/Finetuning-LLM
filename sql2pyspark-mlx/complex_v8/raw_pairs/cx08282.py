from pyspark.sql import functions as F

student_side = (
    df_students
    .select(
        F.lit("student").alias("src"),
        "id",
        "gpa",
        F.col("enrolled_on").alias("event_date"),
    )
)

student_rows = (
    df_students
    .select(
        F.lit("student_2").alias("src"),
        "id",
        "gpa",
        F.col("enrolled_on").alias("event_date"),
    )
)

all_events = student_side.unionByName(student_rows)

result = (
    all_events
    .groupBy("src")
    .agg(
        F.count("*").alias("n_rows"),
        F.sum("gpa").alias("total_gpa"),
        F.max("event_date").alias("latest_event_date"),
    )
    .orderBy("src")
)
