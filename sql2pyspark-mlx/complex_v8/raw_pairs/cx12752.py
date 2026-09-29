from pyspark.sql import functions as F

student_side = df_students.select(F.col("id").alias("entity_id"), F.col("enrolled_on").alias("day"))

student_side_2 = (
    df_students
    .filter((F.year(F.col("enrolled_on")) == 2024) & (F.col("cohort").isNotNull()))
    .select(F.col("id").alias("entity_id"), F.col("enrolled_on").alias("day"))
)

student_rows = (
    df_students
    .filter(F.col("major").isin("Art", "Bio"))
    .select(F.col("id").alias("entity_id"), F.col("enrolled_on").alias("day"))
)

result = (
    student_side
    .unionByName(student_side_2)
    .unionByName(student_rows)
    .orderBy("entity_id")
)
