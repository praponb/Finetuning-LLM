from pyspark.sql import functions as F

student_rows = (
    df_students
    .filter((F.col("is_international")) & (F.col("enrolled_on").isNotNull()))
    .select(
        "id",
        "gpa",
        F.col("enrolled_on").alias("day"),
        F.lit("student").alias("record_type"),
    )
)

student_side = (
    df_students
    .select(
        "id",
        "gpa",
        "enrolled_on",
        F.lit("student_2").alias("record_type"),
    )
)

combined = student_rows.union(student_side)

result = (
    combined
    .groupBy("record_type")
    .agg(
        F.count("*").alias("n_rows"),
        F.sum("gpa").alias("total_gpa"),
        F.max("day").alias("latest_day"),
    )
    .orderBy("record_type")
)
