from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_course_credits_by_enrolled_a = Window.partitionBy("course_credits").orderBy("enrolled_at", "id")
w_course_credits_enrolled_at_m = Window.partitionBy("course_credits", F.date_trunc("month", "enrolled_at"))

recent = (
    df_enrollments
    .filter((F.col("enrolled_at") >= F.lit("2025-01-19 15:00:00").cast("timestamp")) & (F.col("score") <= 74.9))
    .select(
        "id",
        "student_id",
        "course_id",
        "term",
        "score",
        "grade_status",
        "enrolled_at",
    )
)

joined = (
    recent.alias("s")
    .join(df_courses.alias("c"), F.col("c.id") == F.col("s.course_id"), "left")
    .select(
        "s.id",
        "s.student_id",
        "s.course_id",
        "s.term",
        "s.score",
        "s.grade_status",
        "s.enrolled_at",
        F.col("c.credits").alias("course_credits"),
        F.col("c.dept").alias("course_dept"),
    )
)

metrics = (
    joined
    .select(
        "*",
        F.row_number().over(w_course_credits_by_enrolled_a).alias("rn"),
        F.round(F.percent_rank().over(w_course_credits_by_enrolled_a), 3).alias("enrolled_at_percent_rank"),
        F.round(F.avg("score").over(w_course_credits_enrolled_at_m), 2).alias("course_credits_enrolled_at_month_avg_score"),
    )
)

joined_course = (
    metrics.alias("s")
    .join(df_courses.alias("c"), F.col("c.id") == F.col("s.course_id"), "inner")
    .join(df_students.alias("st"), F.col("st.id") == F.col("s.student_id"), "inner")
    .select(
        "s.id",
        "s.student_id",
        "s.course_id",
        "s.term",
        "s.enrolled_at",
        "s.course_credits",
        "s.course_dept",
        "s.rn",
        "s.course_credits_enrolled_at_month_avg_score",
        F.col("c.level").alias("course_level"),
        "st.student_name",
        F.col("st.is_international").alias("student_is_international"),
    )
)

line_level_enrollments = (
    joined_course.alias("s")
    .join(df_enrollments.alias("e"), (F.col("e.course_id") == F.col("s.course_id")) & (F.col("e.term").isin("2024T2", "2025T1", "2025T2")), "inner")
    .select(
        "s.id",
        "s.student_id",
        "s.course_id",
        "s.term",
        "s.enrolled_at",
        "s.course_dept",
        "s.rn",
        "s.course_level",
        "s.student_is_international",
        F.col("e.id").alias("enrollment_id"),
        F.col("e.term").alias("enrollment_term"),
        F.col("e.enrolled_at").alias("enrollment_enrolled_at"),
        F.col("e.student_id").alias("enrollment_student_id"),
    )
)

enriched = (
    line_level_enrollments.alias("s")
    .join(df_students.alias("st"), (F.col("st.id") == F.col("s.student_id")) & (F.col("st.major") != "Math"), "inner")
    .select("s.*", F.col("st.cohort").alias("student_cohort"))
)

result = (
    enriched
    .filter(F.col("student_cohort") == "C2023")
    .select(
        "id",
        "course_id",
        "term",
        "enrolled_at",
        "course_dept",
        "rn",
        "course_level",
        "student_is_international",
        "enrollment_id",
        "enrollment_term",
        "enrollment_enrolled_at",
        "student_cohort",
    )
    .orderBy(F.col("enrollment_enrolled_at").asc())
)
