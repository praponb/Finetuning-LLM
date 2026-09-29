from pyspark.sql import functions as F

filtered = (
    df_students
    .filter(F.col("gpa") >= 2.7)
    .select(
        "id",
        "student_name",
        "cohort",
        "major",
        "enrolled_on",
        "gpa",
        "is_international",
        F.length(F.trim(F.col("cohort"))).alias("cohort_len"),
    )
)

student_side = (
    filtered
    .select(
        F.col("cohort_len").alias("key_id"),
        F.col("enrolled_on").alias("event_date"),
    )
)

student_side_2 = (
    df_students
    .filter(~F.col("cohort").isin("C2023", "C2024"))
    .select(F.col("id").alias("key_id"), F.col("enrolled_on").alias("event_date"))
)

student_side_3 = (
    df_students
    .select(F.col("id").alias("key_id"), F.col("enrolled_on").alias("event_date"))
)

unioned = (
    student_side
    .unionByName(student_side_2)
    .unionByName(student_side_3)
)

result = (
    unioned
    .groupBy("key_id")
    .agg(
        F.count("*").alias("n_rows"),
        F.max("event_date").alias("latest_event_date"),
    )
    .orderBy("key_id")
)
