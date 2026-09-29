from pyspark.sql import functions as F

enrollment_side = (
    df_enrollments
    .filter((F.col("course_id").isNotNull()) & (~F.col("term").isin("2024T1", "2025T2")))
    .select(
        F.col("course_id").alias("entity_id"),
        F.col("id").alias("entity_id_2"),
        F.lit("enrollment").alias("src"),
    )
)

course_side = (
    df_courses
    .filter((~F.col("dept").isin("Art", "CS")) & (F.col("level").isin("advanced", "intro")))
    .select(
        F.col("credits").alias("entity_id"),
        F.col("id").alias("entity_id_2"),
        F.lit("course").alias("src"),
    )
)

course_side_2 = (
    df_courses
    .select(
        F.col("id").alias("entity_id"),
        F.col("credits").alias("entity_id_2"),
        F.lit("course").alias("src"),
    )
)

result = (
    enrollment_side
    .union(course_side)
    .union(course_side_2)
    .orderBy(F.col("entity_id").desc(), "entity_id_2")
)
