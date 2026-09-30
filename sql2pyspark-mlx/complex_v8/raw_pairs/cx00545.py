from pyspark.sql import functions as F

valid = (
    df_students
    .filter(F.col("gpa") >= 2.7)
    .select(
        "id",
        "student_name",
        "major",
        "enrolled_on",
        "gpa",
        "is_international",
        F.date_trunc("year", F.col("enrolled_on")).alias("enrolled_on_year"),
        F.year(F.col("enrolled_on")).alias("enrolled_on_year_2"),
    )
)

enriched = valid.select("*", F.month(F.col("enrolled_on")).alias("enrolled_on_month"))

with_activity = (
    enriched.alias("s")
    .join(df_enrollments.alias("e"), (F.col("e.student_id") == F.col("s.id")) & (F.col("e.course_id") <= 5), "left_anti")
    .filter(~F.col("major").isin("Bio", "Biz"))
)

line_level_enrollments = (
    with_activity.alias("s")
    .join(df_enrollments.alias("e"), F.col("e.student_id") == F.col("s.id"), "inner")
    .select(
        "s.id",
        "s.student_name",
        "s.major",
        "s.enrolled_on",
        "s.enrolled_on_month",
        F.col("e.id").alias("enrollment_id"),
        F.col("e.course_id").alias("enrollment_course_id"),
        F.col("e.score").alias("enrollment_score"),
        F.col("e.term").alias("enrollment_term"),
    )
)

result = (
    line_level_enrollments
    .filter(F.col("enrollment_id").isNotNull())
    .select(
        "id",
        "student_name",
        "major",
        "enrolled_on",
        "enrollment_id",
        "enrollment_course_id",
        "enrollment_score",
        "enrollment_term",
        (F.col("enrolled_on_month") - F.col("enrollment_score")).alias("enrolled_on_month_minus_enrollment_score"),
    )
    .orderBy("enrolled_on_month_minus_enrollment_score")
)
