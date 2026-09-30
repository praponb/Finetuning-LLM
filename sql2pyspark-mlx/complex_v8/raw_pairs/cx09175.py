from pyspark.sql import functions as F

base = (
    df_courses
    .select(
        "id",
        "course_code",
        "dept",
        "credits",
        "level",
        F.concat_ws(" / ", F.col("course_code"), F.col("dept")).alias("course_code_dept_label"),
        F.lower(F.trim(F.col("level"))).alias("level_clean"),
        F.length(F.trim(F.col("level"))).alias("level_len"),
    )
)

detail_rows = (
    base.alias("s")
    .join(df_enrollments.alias("e"), F.col("e.course_id") == F.col("s.id"), "left")
    .select(
        "s.id",
        "s.dept",
        "s.level",
        "s.course_code_dept_label",
        F.col("e.id").alias("enrollment_id"),
        F.col("e.term").alias("enrollment_term"),
        F.col("e.score").alias("enrollment_score"),
        F.col("e.student_id").alias("enrollment_student_id"),
        F.col("e.enrolled_at").alias("enrollment_enrolled_at"),
    )
)

enrollment_student_id_stats = (
    detail_rows
    .groupBy("enrollment_student_id")
    .agg(F.avg("enrollment_score").alias("avg_enrollment_score"))
)

above_average = (
    detail_rows.alias("s")
    .join(enrollment_student_id_stats.alias("g"), F.col("g.enrollment_student_id") == F.col("s.enrollment_student_id"), "inner")
    .filter(F.col("s.enrollment_score") > F.col("g.avg_enrollment_score"))
    .select(
        "s.*",
        "g.avg_enrollment_score",
        F.round(F.col("s.enrollment_score") - F.col("g.avg_enrollment_score"), 2).alias("enrollment_score_minus_avg"),
    )
)

result = (
    above_average
    .cube("enrollment_id", "enrollment_student_id", "dept")
    .agg(
        F.grouping_id().alias("grouping_level"),
        F.count(F.when(F.col("level").isin("intro", "core"), 1)).alias("n_level_in_intro_core"),
        F.round(F.sum("avg_enrollment_score") / F.nullif(F.sum("enrollment_score_minus_avg"), F.lit(0)), 3).alias("avg_enrollment_score_to_enrollment_score_minus_a"),
    )
    .orderBy(
        F.col("enrollment_id").asc_nulls_last(),
        F.col("enrollment_student_id").asc_nulls_last(),
        F.col("dept").asc_nulls_last(),
    )
)
