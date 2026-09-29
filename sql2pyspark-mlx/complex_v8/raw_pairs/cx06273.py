from pyspark.sql import functions as F

cleaned_enrollments = (
    df_enrollments
    .filter((F.col("term").isNotNull()) & (F.col("grade_status").isin("INCOMPLETE", "PASS")))
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

without_matches = (
    cleaned_enrollments.alias("s")
    .join(df_enrollments.alias("e"), (F.col("e.course_id") == F.col("s.course_id")) & (F.col("e.term") == "2024T1"), "left_anti")
)

qualified = (
    without_matches.alias("s")
    .join(df_courses.alias("c"), (F.col("c.id") == F.col("s.course_id")) & (F.upper(F.trim(F.col("c.course_code"))).like("A%")) & (F.col("c.level") == "intro"), "left_semi")
)

with_details = (
    qualified.alias("s")
    .join(df_students.alias("st"), (F.col("st.id") == F.col("s.student_id")) & (F.lower(F.col("st.student_name")).like("%ir%")), "inner")
    .join(df_courses.alias("c"), F.col("c.id") == F.col("s.course_id"), "left")
    .filter(F.col("st.enrolled_on") < F.lit("2025-07-03").cast("date"))
    .select(
        "s.id",
        "s.student_id",
        "s.course_id",
        "s.term",
        "s.score",
        "s.enrolled_at",
        F.col("st.enrolled_on").alias("student_enrolled_on"),
        F.col("c.dept").alias("course_dept"),
    )
)

qualified_2 = (
    with_details.alias("s")
    .join(df_enrollments.alias("e"), (F.col("e.student_id") == F.col("s.student_id")) & (F.col("e.grade_status") == "PASS") & (F.col("e.enrolled_at") >= F.lit("2025-01-13 09:00:00").cast("timestamp")), "left_anti")
)

totals = (
    qualified_2
    .groupBy(
        "course_dept",
        F.date_trunc("month", "enrolled_at").alias("enrolled_at_month"),
    )
    .agg(
        F.sum(F.when(F.col("course_dept").isin("Biz", "Bio", "CS"), F.col("score"))).alias("score_course_dept_in_biz_bio_cs"),
        F.percentile_approx("score", 0.5).alias("median_score"),
        F.round(F.sum("score"), 2).alias("total_score"),
        F.datediff(F.max("student_enrolled_on"), F.min("student_enrolled_on")).alias("student_enrolled_on_span_days"),
    )
)

without_matches_2 = (
    totals
    .crossJoin(totals.agg(F.max("total_score").alias("overall_max_total_score")))
    .filter(F.col("total_score") < F.col("overall_max_total_score"))
    .drop("overall_max_total_score")
)

result = (
    without_matches_2
    .select("course_dept", "enrolled_at_month", "score_course_dept_in_biz_bio_cs")
    .orderBy(F.col("course_dept").asc_nulls_last())
)
