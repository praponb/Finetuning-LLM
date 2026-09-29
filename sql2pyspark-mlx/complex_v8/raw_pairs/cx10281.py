from pyspark.sql import functions as F

recent_students = (
    df_students
    .filter((F.col("gpa") >= 2.7) & (F.col("major") == "CS"))
    .select(
        "id",
        "major",
        "enrolled_on",
        "is_international",
        F.upper(F.trim(F.col("student_name"))).alias("student_name_clean"),
        F.coalesce(F.col("gpa"), F.lit(0.0)).alias("gpa_filled"),
    )
)

qualified = (
    recent_students.alias("s")
    .join(df_enrollments.alias("e"), (F.col("e.student_id") == F.col("s.id")) & (~F.col("e.term").isin("2024T2", "2025T1")) & (F.col("e.grade_status") == "FAIL"), "left_anti")
)

result = (
    qualified
    .filter(F.col("gpa_filled") <= 3.2)
    .select(
        "id",
        "major",
        "enrolled_on",
        "is_international",
        F.when(F.col("major").isin("Math", "CS"), "group_a").when(F.col("major") == "Art", "group_b").otherwise("other").alias("major_group"),
    )
    .orderBy(F.col("major_group").desc(), F.col("id").desc_nulls_first())
)
