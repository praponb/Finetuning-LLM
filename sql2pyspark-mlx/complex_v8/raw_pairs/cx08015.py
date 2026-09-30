from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_first_enrollment_enrolled_at = Window.partitionBy(F.date_trunc("month", "first_enrollment_enrolled_at")).orderBy("enrolled_at", "id", "enrollment_id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_first_enrollment_enrolled_at_2 = Window.partitionBy(F.date_trunc("month", "first_enrollment_enrolled_at")).orderBy("enrolled_at", "id", "enrollment_id")

base = (
    df_enrollments
    .filter(F.col("student_id").isNotNull())
    .select(
        "id",
        "student_id",
        "course_id",
        "term",
        "score",
        "grade_status",
        "enrolled_at",
        F.round(F.col("score") * 1.1, 2).alias("score_adj"),
    )
)

joined = (
    base.alias("s")
    .join(df_students.alias("st"), F.col("st.id") == F.col("s.student_id"), "inner")
    .join(df_courses.alias("c"), F.col("c.id") == F.col("s.course_id"), "left")
    .select(
        "s.id",
        "s.student_id",
        "s.course_id",
        "s.term",
        "s.score",
        "s.grade_status",
        "s.enrolled_at",
        "s.score_adj",
        F.col("st.gpa").alias("student_gpa"),
        F.col("st.cohort").alias("student_cohort"),
        F.col("st.major").alias("student_major"),
        F.col("c.level").alias("course_level"),
        F.col("c.dept").alias("course_dept"),
    )
)

enriched_course = (
    joined.alias("s")
    .join(df_courses.alias("c"), F.col("c.id") == F.col("s.course_id"), "inner")
    .join(df_students.alias("st"), F.col("st.id") == F.col("s.student_id"), "inner")
    .filter(F.col("c.level").isNotNull())
    .select(
        "s.id",
        "s.student_id",
        "s.course_id",
        "s.score",
        "s.grade_status",
        "s.enrolled_at",
        "s.score_adj",
        "s.student_gpa",
        "s.student_cohort",
        "s.student_major",
        "s.course_dept",
        F.col("c.credits").alias("course_credits"),
        F.col("c.level").alias("course_level_2"),
        F.col("st.cohort").alias("student_cohort_2"),
        F.col("st.major").alias("student_major_2"),
    )
)

annotated = (
    enriched_course
    .filter(F.col("student_cohort_2").isin("C2021", "C2023"))
    .select(
        "id",
        "student_id",
        "score",
        "grade_status",
        "enrolled_at",
        "score_adj",
        "student_gpa",
        "student_major",
        "course_level_2",
        "student_major_2",
        F.concat_ws("-", F.col("grade_status"), F.col("student_cohort_2")).alias("grade_status_student_cohort_2_label"),
        F.coalesce(F.col("score_adj"), F.lit(0.0)).alias("score_adj_filled"),
    )
)

enrollment_stats = (
    df_enrollments
    .groupBy("student_id")
    .agg(
        F.count("*").alias("enrollment_count"),
        F.min("enrolled_at").alias("first_enrollment_enrolled_at"),
    )
)

with_stats = (
    annotated.alias("s")
    .join(enrollment_stats.alias("e"), F.col("e.student_id") == F.col("s.student_id"), "left")
    .select(
        "s.*",
        F.coalesce(F.col("e.enrollment_count"), F.lit(0)).alias("enrollment_count"),
        "e.first_enrollment_enrolled_at",
    )
)

exploded_detail = (
    with_stats.alias("s")
    .join(df_enrollments.alias("e"), (F.col("e.student_id") == F.col("s.student_id")) & (F.col("e.course_id") >= 10), "inner")
    .select(
        "s.id",
        "s.student_id",
        "s.score",
        "s.enrolled_at",
        "s.student_major",
        "s.course_level_2",
        "s.student_major_2",
        "s.grade_status_student_cohort_2_label",
        "s.enrollment_count",
        "s.first_enrollment_enrolled_at",
        F.col("e.id").alias("enrollment_id"),
        F.col("e.term").alias("enrollment_term"),
        F.col("e.course_id").alias("enrollment_course_id"),
        F.col("e.grade_status").alias("enrollment_grade_status"),
    )
)

with_windows = (
    exploded_detail
    .select(
        "id",
        "score",
        "enrolled_at",
        "course_level_2",
        "student_major_2",
        "grade_status_student_cohort_2_label",
        "enrollment_count",
        "first_enrollment_enrolled_at",
        "enrollment_id",
        F.first("enrolled_at").over(w_first_enrollment_enrolled_at).alias("first_enrolled_at"),
        F.row_number().over(w_first_enrollment_enrolled_at_2).alias("enrolled_at_seq"),
    )
)

result = (
    with_windows
    .groupBy("student_major_2", "enrollment_id")
    .pivot("course_level_2", ["core", "advanced", "intro"])
    .agg(F.sum("score"))
)
