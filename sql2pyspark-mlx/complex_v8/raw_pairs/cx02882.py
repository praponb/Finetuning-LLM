from pyspark.sql import functions as F

base_students = (
    df_students
    .filter(F.col("enrolled_on").isNotNull())
    .select(
        "id",
        "student_name",
        "cohort",
        "major",
        "enrolled_on",
        "gpa",
        F.round(F.col("gpa") * 1.5, 2).alias("gpa_adj"),
    )
)

student_rows = (
    base_students
    .filter((F.col("enrolled_on").isNotNull()) & (F.col("cohort") == "C2023"))
    .select(
        F.lit("student").alias("record_type"),
        "id",
        F.col("student_name").alias("name"),
    )
)

enrollment_side = (
    df_enrollments
    .filter(F.col("student_id").isNotNull())
    .select(
        F.lit("enrollment").alias("record_type"),
        "id",
        F.col("term").alias("name"),
    )
)

combined = student_rows.unionByName(enrollment_side)

result = (
    combined
    .groupBy("record_type")
    .agg(F.count("*").alias("n_rows"), F.sum("id").alias("total_id"))
    .orderBy("record_type")
)
