from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_enrollment_id_by_student_gpa = Window.partitionBy("enrollment_id").orderBy(F.col("student_gpa").desc(), "id", "enrollment_id").rowsBetween(-2, -1)
w_dept_enrollment_grade_status = Window.partitionBy("dept", "enrollment_grade_status")

scoped = (
    df_courses
    .filter((~F.col("level").isin("advanced", "core")) & (F.upper(F.trim(F.col("course_code"))).like("C%")))
    .select(
        "id",
        "course_code",
        "dept",
        "credits",
        "level",
    )
)

line_level = (
    scoped.alias("s")
    .join(df_enrollments.alias("e"), F.col("e.course_id") == F.col("s.id"), "left")
    .select(
        "s.id",
        "s.dept",
        "s.level",
        F.col("e.id").alias("enrollment_id"),
        F.col("e.student_id").alias("enrollment_student_id"),
        F.col("e.enrolled_at").alias("enrollment_enrolled_at"),
        F.col("e.grade_status").alias("enrollment_grade_status"),
        F.col("e.score").alias("enrollment_score"),
    )
)

with_details_enrollment = (
    line_level.alias("s")
    .join(df_enrollments.alias("e"), (F.col("e.id") == F.col("s.enrollment_id")) & (F.col("e.grade_status") == "PASS"), "left")
    .select("s.*", F.col("e.enrolled_at").alias("enrollment_enrolled_at_2"))
)

joined_enrollment = (
    with_details_enrollment.alias("s")
    .join(df_enrollments.alias("e"), F.col("e.id") == F.col("s.enrollment_id"), "left")
    .join(df_students.alias("st"), F.col("st.id") == F.col("e.student_id"), "left")
    .select(
        "s.id",
        "s.dept",
        "s.level",
        "s.enrollment_id",
        "s.enrollment_student_id",
        "s.enrollment_enrolled_at",
        "s.enrollment_grade_status",
        "s.enrollment_score",
        "s.enrollment_enrolled_at_2",
        F.col("e.enrolled_at").alias("enrollment_enrolled_at_3"),
        F.col("e.student_id").alias("enrollment_student_id_2"),
        F.col("st.gpa").alias("student_gpa"),
    )
)

ranked = (
    joined_enrollment
    .select(
        "*",
        F.round(F.avg("enrollment_score").over(w_enrollment_id_by_student_gpa), 2).alias("enrollment_score_avg_2_prior"),
        F.round(F.col("enrollment_score") / F.nullif(F.sum("enrollment_score").over(w_dept_enrollment_grade_status), F.lit(0)), 4).alias("enrollment_score_share_of_dept_enrollment_grade_"),
        F.sum("enrollment_score").over(w_dept_enrollment_grade_status).alias("dept_enrollment_grade_status_sum_enrollment_scor"),
    )
)

dept_stats = (
    ranked
    .groupBy("dept")
    .agg(F.avg("enrollment_score_avg_2_prior").alias("avg_enrollment_score_avg_2_prior"))
)

outliers = (
    ranked.alias("s")
    .join(dept_stats.alias("g"), F.col("g.dept") == F.col("s.dept"), "inner")
    .filter(F.col("s.enrollment_score_avg_2_prior") > F.col("g.avg_enrollment_score_avg_2_prior"))
    .select(
        "s.*",
        "g.avg_enrollment_score_avg_2_prior",
        F.round(F.col("s.enrollment_score_avg_2_prior") - F.col("g.avg_enrollment_score_avg_2_prior"), 2).alias("enrollment_score_avg_2_prior_minus_avg"),
    )
)

result = (
    outliers
    .rollup("dept", "enrollment_grade_status")
    .agg(
        F.grouping("dept").alias("is_total_dept"),
        F.min("enrollment_score").alias("min_enrollment_score"),
        F.max("enrollment_enrolled_at").alias("last_enrollment_enrolled_at"),
        F.max("dept_enrollment_grade_status_sum_enrollment_scor").alias("max_dept_enrollment_grade_status_sum_enrollment_"),
    )
    .select(
        F.coalesce(F.col("dept"), F.lit("ALL")).alias("dept"),
        F.coalesce(F.col("enrollment_grade_status"), F.lit("ALL")).alias("enrollment_grade_status"),
        "is_total_dept",
        "min_enrollment_score",
        "last_enrollment_enrolled_at",
        "max_dept_enrollment_grade_status_sum_enrollment_",
    )
    .orderBy(
        F.col("dept").asc_nulls_last(),
        F.col("enrollment_grade_status").asc_nulls_last(),
    )
)
