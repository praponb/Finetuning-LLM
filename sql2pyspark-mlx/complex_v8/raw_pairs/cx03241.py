from pyspark.sql import functions as F

valid_enrollments = (
    df_enrollments
    .filter((F.hour(F.col("enrolled_at")).between(10, 13)) & (F.col("score") < 77.0))
    .select(
        "id",
        "student_id",
        "course_id",
        "term",
        "score",
        "grade_status",
        "enrolled_at",
        (F.floor(F.col("score") / 25) * 25).alias("score_bucket"),
        F.when(F.col("score") >= 63.9, "HIGH").when(F.col("score") >= 32.7, "MEDIUM").otherwise("LOW").alias("score_tier"),
        F.concat_ws(" / ", F.col("grade_status"), F.col("term")).alias("grade_status_term_label"),
    )
)

scored = (
    valid_enrollments
    .filter(F.col("student_id") >= 11)
    .select(
        "*",
        F.when(F.col("grade_status").isin("WITHDRAWN", "PASS"), "group_a").when(F.col("grade_status") == "INCOMPLETE", "group_b").otherwise("other").alias("grade_status_group"),
        F.datediff(F.lit("2025-06-30").cast("date"), F.col("enrolled_at")).alias("days_since_enrolled_at"),
    )
)

joined_course = (
    scored.alias("s")
    .join(df_courses.alias("c"), (F.col("c.id") == F.col("s.course_id")) & (F.col("c.credits") >= 2), "left")
    .select(
        "s.id",
        "s.student_id",
        "s.course_id",
        "s.grade_status",
        "s.score_tier",
        "s.grade_status_term_label",
        "s.grade_status_group",
        "s.days_since_enrolled_at",
        F.col("c.dept").alias("course_dept"),
        "c.course_code",
        F.col("c.credits").alias("course_credits"),
    )
)

result = (
    joined_course
    .select("id", "days_since_enrolled_at", "course_credits")
    .unpivot(
        ids=["id"],
        values=["days_since_enrolled_at", "course_credits"],
        variableColumnName="metric_name",
        valueColumnName="value",
    )
    .filter(F.col("value").isNotNull())
    .orderBy("id", "metric_name")
)
