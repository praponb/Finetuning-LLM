from pyspark.sql import functions as F

student_rows = (
    df_students
    .filter((F.col("cohort") == "C2024") & (F.length(F.trim(F.col("student_name"))) >= 5))
    .select(
        F.col("id").alias("entity_id"),
        "enrolled_on",
        F.lit("student").alias("origin"),
    )
)

student_side = (
    df_students
    .filter(F.col("major") != "Bio")
    .select("id", "enrolled_on", F.lit("student_2").alias("origin"))
)

student_rows_2 = (
    df_students
    .filter((F.col("enrolled_on").between(F.lit("2024-04-04").cast("date"), F.lit("2025-08-27").cast("date"))) & (F.col("major") != "CS"))
    .select("id", "enrolled_on", F.lit("student_3").alias("origin"))
)

combined = (
    student_rows
    .union(student_side)
    .union(student_rows_2)
)

result = (
    combined
    .groupBy("origin")
    .agg(
        F.count("*").alias("n_rows"),
        F.sum("entity_id").alias("total_entity_id"),
        F.max("enrolled_on").alias("latest_enrolled_on"),
    )
    .orderBy("origin")
)
