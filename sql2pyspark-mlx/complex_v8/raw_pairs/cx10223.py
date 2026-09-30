from pyspark.sql import functions as F

base = (
    df_students
    .filter((~F.col("is_international")) & (F.col("enrolled_on") < F.lit("2025-07-18").cast("date")))
    .select(
        "id",
        "student_name",
        "cohort",
        "major",
        "enrolled_on",
        "gpa",
    )
)

student_rows = (
    base
    .filter(F.year(F.col("enrolled_on")) == 2025)
    .select("cohort")
)

enrollment_side = (
    df_enrollments
    .filter((F.col("course_id") <= 5) & (F.col("term").isNotNull()))
    .select(F.col("grade_status").alias("cohort"))
)

result = (
    student_rows
    .intersect(enrollment_side)
    .orderBy(F.col("cohort").desc())
)
