from pyspark.sql import functions as F

scoped = (
    df_courses
    .filter((F.col("credits") <= 3) & (F.col("level").isin("core", "advanced")))
    .select(
        "id",
        "dept",
        "credits",
        "level",
        F.coalesce(F.col("dept"), F.lit("UNKNOWN")).alias("dept_or_unknown"),
    )
)

course_side = scoped.select(F.col("id").alias("entity_id"), "level")

student_side = df_students.select(F.col("id").alias("entity_id"), F.col("cohort").alias("level"))

student_side_2 = (
    df_students
    .filter(~F.col("is_international"))
    .select(F.col("id").alias("entity_id"), F.col("student_name").alias("level"))
)

result = (
    course_side
    .unionByName(student_side)
    .unionByName(student_side_2)
    .orderBy("entity_id", "level")
)
