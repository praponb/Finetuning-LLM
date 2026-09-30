from pyspark.sql import functions as F

filtered = (
    df_courses
    .filter(F.col("dept").isin("CS", "Art"))
    .select(
        "id",
        "course_code",
        "credits",
        "level",
    )
)

course_side = (
    filtered
    .filter((F.upper(F.trim(F.col("course_code"))).like("M%")) & (F.col("level") != "advanced"))
    .select(F.col("id").alias("entity_id"))
)

student_side = df_students.select("id")

result = (
    course_side
    .subtract(student_side)
    .orderBy("entity_id")
)
