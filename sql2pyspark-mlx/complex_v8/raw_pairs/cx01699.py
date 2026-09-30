from pyspark.sql import functions as F

base = (
    df_students
    .filter(F.col("student_name").isNotNull())
    .select(
        "id",
        "cohort",
        "major",
        "enrolled_on",
        F.length(F.trim(F.col("cohort"))).alias("cohort_len"),
    )
)

student_side = (
    base
    .filter(F.year(F.col("enrolled_on")) == 2025)
    .select(F.col("major").alias("label"), F.col("cohort").alias("name"))
)

enrollment_rows = (
    df_enrollments
    .filter(F.col("term") == "2025T2")
    .select("term", "grade_status")
)

result = (
    student_side
    .union(enrollment_rows)
    .distinct()
    .orderBy(F.col("label").desc(), "name")
)
