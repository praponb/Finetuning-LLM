from pyspark.sql import functions as F

prepared = (
    df_enrollments
    .filter(F.col("score") >= 17.4)
    .select(
        "id",
        "student_id",
        "course_id",
        "term",
        "score",
        "grade_status",
        "enrolled_at",
        F.abs(F.col("score") - 57.8).alias("score_distance"),
        (F.floor(F.col("score") / 25) * 25).alias("score_bucket"),
        F.coalesce(F.col("score"), F.lit(0.0)).alias("score_filled"),
    )
)

with_parent = (
    prepared.alias("s")
    .join(df_students.alias("st"), (F.col("st.id") == F.col("s.student_id")) & (F.col("st.enrolled_on") >= F.lit("2024-04-04").cast("date")), "inner")
    .join(df_courses.alias("c"), F.col("c.id") == F.col("s.course_id"), "inner")
    .filter(~F.col("st.is_international"))
    .select(
        "s.id",
        "s.student_id",
        "s.course_id",
        "s.term",
        "s.grade_status",
        "s.enrolled_at",
        "s.score_distance",
        "s.score_bucket",
        F.col("st.cohort").alias("student_cohort"),
        F.col("st.is_international").alias("student_is_international"),
        "c.course_code",
    )
)

with_activity = (
    with_parent.alias("s")
    .join(df_students.alias("st"), (F.col("st.id") == F.col("s.student_id")) & (F.col("st.enrolled_on") >= F.lit("2024-05-18").cast("date")) & (F.col("st.gpa") > 2.5), "left_anti")
)

result = (
    with_activity
    .groupBy("term", "student_id")
    .pivot("grade_status", ["INCOMPLETE", "WITHDRAWN"])
    .agg(F.min("score_bucket"))
)
