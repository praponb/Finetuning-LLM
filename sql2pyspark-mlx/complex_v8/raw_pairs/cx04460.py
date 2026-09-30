from pyspark.sql import functions as F

prepared = df_students

student_side = prepared.select("id", F.col("enrolled_on").alias("event_date"))

student_side_2 = df_students.select("id", F.col("enrolled_on").alias("event_date"))

all_events = (
    student_side
    .union(student_side_2)
    .distinct()
)

result = (
    all_events
    .groupBy("id")
    .agg(
        F.count("*").alias("n_rows"),
        F.max("event_date").alias("latest_event_date"),
    )
    .orderBy("id")
)
