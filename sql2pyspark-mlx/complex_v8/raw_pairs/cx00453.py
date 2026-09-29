from pyspark.sql import functions as F

valid = (
    df_students
    .filter((F.year(F.col("enrolled_on")) == 2025) & (F.upper(F.trim(F.col("student_name"))).like("I%")))
    .select(
        "id",
        "cohort",
        "major",
        "gpa",
        "is_international",
        F.month(F.col("enrolled_on")).alias("enrolled_on_month"),
    )
)

qualified = (
    valid.alias("s")
    .join(df_enrollments.alias("e"), (F.col("e.student_id") == F.col("s.id")) & (F.col("e.grade_status") == "INCOMPLETE") & (F.col("e.term") == "2024T1"), "left_anti")
    .filter(F.col("gpa") < 3.2)
)

result = (
    qualified
    .groupBy("cohort", "enrolled_on_month")
    .pivot("major", ["Art", "CS"])
    .agg(F.min("gpa"))
    .orderBy("cohort", "enrolled_on_month")
)
