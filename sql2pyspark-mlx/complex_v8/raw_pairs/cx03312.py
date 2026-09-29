from pyspark.sql import functions as F

cleaned = (
    df_students
    .filter((~F.col("is_international")) & (F.col("gpa") >= 2.0))
    .select(
        "id",
        "student_name",
        "cohort",
        "enrolled_on",
        "is_international",
        F.concat(F.col("major"), F.lit("#"), F.col("id").cast("string")).alias("major_ref"),
    )
)

student_side = (
    cleaned
    .select(
        F.lit("student").alias("src"),
        F.col("major_ref").alias("label"),
        F.col("enrolled_on").alias("event_date"),
    )
)

student_side_2 = (
    df_students
    .filter(F.col("gpa") > 2.7)
    .select(
        F.lit("student_2").alias("src"),
        F.col("student_name").alias("label"),
        F.col("enrolled_on").alias("event_date"),
    )
)

result = (
    student_side
    .unionByName(student_side_2)
    .distinct()
)
