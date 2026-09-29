from pyspark.sql import functions as F

in_window = (
    df_enrollments.alias("a")
    .join(
        df_enrollments.alias("b"),
        (F.col("b.student_id") == F.col("a.student_id"))
        & (F.col("b.enrolled_at") < F.col("a.enrolled_at"))
        & (F.col("b.enrolled_at") >= F.col("a.enrolled_at") - F.expr("INTERVAL 7 DAYS")),
        "left",
    )
    .groupBy("a.id", "a.student_id", "a.enrolled_at")
    .agg(
        F.count("b.id").alias("prior_rows_7d"),
        F.sum("b.score").alias("prior_score_7d"),
    )
)

line_level = (
    in_window.alias("s")
    .join(df_enrollments.alias("e"), F.col("e.student_id") == F.col("s.student_id"), "left")
    .select(
        "s.id",
        "s.student_id",
        "s.prior_rows_7d",
        "s.prior_score_7d",
        F.col("e.id").alias("enrollment_id"),
        F.col("e.course_id").alias("enrollment_course_id"),
        F.col("e.enrolled_at").alias("enrollment_enrolled_at"),
        F.col("e.term").alias("enrollment_term"),
        F.col("e.score").alias("enrollment_score"),
    )
)

joined = (
    line_level.alias("s")
    .join(df_courses.alias("c"), F.col("c.id") == F.col("s.enrollment_course_id"), "inner")
    .select(
        "s.id",
        "s.student_id",
        "s.prior_rows_7d",
        "s.prior_score_7d",
        "s.enrollment_id",
        "s.enrollment_course_id",
        "s.enrollment_enrolled_at",
        "s.enrollment_score",
        F.col("c.level").alias("course_level"),
        F.col("c.dept").alias("course_dept"),
        "c.course_code",
    )
)

result = (
    joined
    .rollup("enrollment_id", "course_dept", "course_level")
    .agg(
        F.max("enrollment_enrolled_at").alias("last_enrollment_enrolled_at"),
        F.round(F.avg("enrollment_score"), 2).alias("avg_enrollment_score"),
    )
    .orderBy(
        F.col("enrollment_id").asc_nulls_last(),
        F.col("course_dept").asc_nulls_last(),
        F.col("course_level").asc_nulls_last(),
    )
)
