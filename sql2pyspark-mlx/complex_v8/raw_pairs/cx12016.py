from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_course_id_enrollment_course = Window.partitionBy("course_id", "enrollment_course_id")
w_course_id_by_enrollment_scor = Window.partitionBy("course_id").orderBy("enrollment_score_2", "id", "enrollment_id", "enrollment_id_2").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_course_id_by_enrollment_scor_2 = Window.partitionBy("course_id").orderBy("enrollment_score_2", "id", "enrollment_id", "enrollment_id_2").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)

scoped_enrollments = (
    df_enrollments
    .filter(F.month(F.col("enrolled_at")).isin(1, 2))
    .select(
        "id",
        "student_id",
        "course_id",
        "term",
        "score",
        "grade_status",
        "enrolled_at",
        F.date_format(F.col("enrolled_at"), "yyyy-MM-dd").alias("enrolled_at_label"),
        (F.col("enrolled_at") + F.expr("INTERVAL 7 DAYS")).alias("enrolled_at_plus_7d"),
    )
)

exploded_detail = (
    scoped_enrollments.alias("s")
    .join(df_enrollments.alias("e"), F.col("e.student_id") == F.col("s.student_id"), "inner")
    .select(
        "s.id",
        "s.student_id",
        "s.course_id",
        "s.term",
        "s.score",
        "s.grade_status",
        "s.enrolled_at_label",
        "s.enrolled_at_plus_7d",
        F.col("e.id").alias("enrollment_id"),
        F.col("e.term").alias("enrollment_term"),
        F.col("e.score").alias("enrollment_score"),
    )
)

enrollment_stats = (
    df_enrollments
    .groupBy("student_id")
    .agg(F.sum("score").alias("total_enrollment_score"))
)

summarized_rows = (
    exploded_detail.alias("s")
    .join(enrollment_stats.alias("e"), F.col("e.student_id") == F.col("s.student_id"), "left")
    .select("s.*", "e.total_enrollment_score")
)

line_level = (
    summarized_rows.alias("s")
    .join(df_enrollments.alias("e"), F.col("e.student_id") == F.col("s.student_id"), "left")
    .select(
        "s.id",
        "s.student_id",
        "s.course_id",
        "s.term",
        "s.score",
        "s.enrolled_at_label",
        "s.enrolled_at_plus_7d",
        "s.enrollment_id",
        "s.enrollment_score",
        "s.total_enrollment_score",
        F.col("e.id").alias("enrollment_id_2"),
        F.col("e.score").alias("enrollment_score_2"),
        F.col("e.enrolled_at").alias("enrollment_enrolled_at"),
        F.col("e.course_id").alias("enrollment_course_id"),
    )
)

sequenced = (
    line_level
    .select(
        "*",
        F.sum("total_enrollment_score").over(w_course_id_enrollment_course).alias("course_id_enrollment_course_id_sum_total_enrollm"),
        F.last("enrollment_score", ignorenulls=True).over(w_course_id_by_enrollment_scor).alias("last_known_enrollment_score"),
        F.count(F.lit(1)).over(w_course_id_enrollment_course).alias("rows_in_course_id_enrollment_course_id"),
        F.nth_value("enrollment_score_2", 2).over(w_course_id_by_enrollment_scor_2).alias("second_enrollment_score_2"),
    )
)

enrollment_course_id_stats = (
    sequenced
    .groupBy("enrollment_course_id")
    .agg(F.max("enrollment_score_2").alias("max_enrollment_score_2"))
)

vs_group = (
    sequenced.alias("s")
    .join(enrollment_course_id_stats.alias("g"), F.col("g.enrollment_course_id") == F.col("s.enrollment_course_id"), "inner")
    .filter(F.col("s.enrollment_score_2") == F.col("g.max_enrollment_score_2"))
    .select(
        "s.*",
        "g.max_enrollment_score_2",
        F.round(F.col("s.enrollment_score_2") - F.col("g.max_enrollment_score_2"), 2).alias("enrollment_score_2_minus_max"),
    )
)

with_details = (
    vs_group.alias("s")
    .join(df_students.alias("st"), (F.col("st.id") == F.col("s.student_id")) & (F.col("st.major").isNotNull()), "inner")
    .join(df_courses.alias("c"), F.col("c.id") == F.col("s.enrollment_course_id"), "inner")
    .filter(~F.col("st.cohort").isin("C2024", "C2021"))
    .select(
        "s.id",
        "s.student_id",
        "s.course_id",
        "s.term",
        "s.score",
        "s.enrollment_id",
        "s.enrollment_score",
        "s.enrollment_id_2",
        "s.enrollment_score_2",
        "s.enrollment_enrolled_at",
        "s.enrollment_course_id",
        "s.last_known_enrollment_score",
        "s.rows_in_course_id_enrollment_course_id",
        "s.second_enrollment_score_2",
        "s.max_enrollment_score_2",
        "s.enrollment_score_2_minus_max",
        F.col("st.gpa").alias("student_gpa"),
        F.col("st.is_international").alias("student_is_international"),
        F.col("st.cohort").alias("student_cohort"),
        F.col("c.level").alias("course_level"),
        F.col("c.credits").alias("course_credits"),
    )
)

result = (
    with_details
    .select(
        "id",
        "enrollment_id",
        F.col("enrollment_score_2_minus_max").alias("m_enrollment_score_2_minus_max"),
        F.col("max_enrollment_score_2").alias("m_max_enrollment_score_2"),
        F.col("score").alias("m_score"),
    )
    .unpivot(
        ids=["id", "enrollment_id"],
        values=["m_enrollment_score_2_minus_max", "m_max_enrollment_score_2", "m_score"],
        variableColumnName="measure",
        valueColumnName="metric_value",
    )
    .filter(F.col("metric_value").isNotNull())
)
