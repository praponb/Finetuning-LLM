from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_cohort = Window.partitionBy("cohort")

valid_students = (
    df_students
    .filter(F.upper(F.trim(F.col("student_name"))).like("I%"))
    .select(
        "id",
        "student_name",
        "cohort",
        "major",
        "enrolled_on",
        "gpa",
        "is_international",
        F.lower(F.trim(F.col("student_name"))).alias("student_name_clean"),
    )
)

enrollment_stats = (
    df_enrollments
    .groupBy("student_id")
    .agg(F.min("score").alias("min_enrollment_score"))
)

with_stats = (
    valid_students.alias("s")
    .join(enrollment_stats.alias("e"), F.col("e.student_id") == F.col("s.id"), "left")
    .select("s.*", "e.min_enrollment_score")
)

filtered = (
    with_stats
    .crossJoin(with_stats.agg(F.avg("gpa").alias("overall_avg_gpa")))
    .filter(F.col("gpa") > F.col("overall_avg_gpa"))
    .drop("overall_avg_gpa")
)

with_trends = (
    filtered
    .select(
        "*",
        F.min("gpa").over(w_cohort).alias("cohort_min_gpa"),
        F.round(F.avg("min_enrollment_score").over(w_cohort), 2).alias("cohort_avg_min_enrollment_score"),
    )
)

result = (
    with_trends
    .groupBy("major")
    .pivot("cohort", ["C2023", "C2022"])
    .agg(F.count("cohort_min_gpa"))
    .orderBy("major")
)
