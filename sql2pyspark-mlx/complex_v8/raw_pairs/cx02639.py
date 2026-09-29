from pyspark.sql import functions as F

valid = (
    df_enrollments
    .filter(F.col("score") < 87.8)
    .select(
        "id",
        "student_id",
        "course_id",
        "term",
        "score",
        "enrolled_at",
        F.date_trunc("week", F.col("enrolled_at")).alias("enrolled_at_week"),
    )
)

without_matches = (
    valid.alias("s")
    .join(df_enrollments.alias("e"), F.col("e.course_id") == F.col("s.course_id"), "left_semi")
    .filter(F.col("course_id") <= 2)
)

enriched = (
    without_matches.alias("s")
    .join(df_students.alias("st"), (F.col("st.id") == F.col("s.student_id")) & (~F.col("st.major").isin("Biz", "Math")), "inner")
    .join(df_courses.alias("c"), F.col("c.id") == F.col("s.course_id"), "inner")
    .select(
        "s.*",
        F.col("st.enrolled_on").alias("student_enrolled_on"),
        "st.student_name",
        F.col("st.gpa").alias("student_gpa"),
        "c.course_code",
        F.col("c.dept").alias("course_dept"),
    )
)

joined = (
    enriched.alias("s")
    .join(df_courses.alias("c"), F.col("c.id") == F.col("s.course_id"), "inner")
    .filter(F.col("c.dept").isin("Math", "CS"))
    .select(
        "s.id",
        "s.student_id",
        "s.course_id",
        "s.score",
        "s.enrolled_at_week",
        "s.student_enrolled_on",
        "s.student_gpa",
        "s.course_code",
        "s.course_dept",
        F.col("c.dept").alias("course_dept_2"),
    )
)

result = (
    joined
    .select("id", "student_gpa", "score")
    .unpivot(
        ids=["id"],
        values=["student_gpa", "score"],
        variableColumnName="metric_name",
        valueColumnName="reading",
    )
)
