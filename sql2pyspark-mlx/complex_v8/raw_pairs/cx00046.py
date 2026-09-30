from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_all_by_days_since_enrolled_a = Window.orderBy(F.col("days_since_enrolled_at").desc())
w_student_id_enrollment_grade = Window.partitionBy("student_id", "enrollment_grade_status")

base = (
    df_enrollments
    .filter(F.col("student_id") <= 6)
    .select(
        "id",
        "student_id",
        "course_id",
        "term",
        "score",
        "grade_status",
        "enrolled_at",
        F.coalesce(F.col("score"), F.lit(0.0)).alias("score_filled"),
        F.datediff(F.lit("2026-01-01").cast("date"), F.col("enrolled_at")).alias("days_since_enrolled_at"),
    )
)

enriched = (
    base
    .filter(F.col("course_id").isNotNull())
    .select(
        "id",
        "student_id",
        "days_since_enrolled_at",
        F.coalesce(F.col("grade_status"), F.lit("UNKNOWN")).alias("grade_status_or_unknown"),
    )
)

with_details = (
    enriched.alias("s")
    .join(df_students.alias("st"), F.col("st.id") == F.col("s.student_id"), "left")
    .select(
        "s.*",
        F.col("st.gpa").alias("student_gpa"),
        F.col("st.enrolled_on").alias("student_enrolled_on"),
    )
)

line_level_enrollments = (
    with_details.alias("s")
    .join(df_enrollments.alias("e"), F.col("e.student_id") == F.col("s.student_id"), "inner")
    .select(
        "s.id",
        "s.student_id",
        "s.days_since_enrolled_at",
        "s.grade_status_or_unknown",
        F.col("e.id").alias("enrollment_id"),
        F.col("e.term").alias("enrollment_term"),
        F.col("e.grade_status").alias("enrollment_grade_status"),
    )
)

metrics = (
    line_level_enrollments
    .withColumn("days_since_enrolled_at_rank", F.rank().over(w_all_by_days_since_enrolled_a))
    .withColumn("student_id_enrollment_grade_status_sum_days_sinc", F.sum("days_since_enrolled_at").over(w_student_id_enrollment_grade))
)

enriched_student = (
    metrics.alias("s")
    .join(df_students.alias("st"), F.col("st.id") == F.col("s.student_id"), "left")
    .join(df_enrollments.alias("e"), F.col("e.id") == F.col("s.enrollment_id"), "left")
    .select(
        "s.id",
        "s.student_id",
        "s.days_since_enrolled_at",
        "s.enrollment_id",
        "s.enrollment_term",
        "s.days_since_enrolled_at_rank",
        "s.student_id_enrollment_grade_status_sum_days_sinc",
        "st.student_name",
        F.col("st.enrolled_on").alias("student_enrolled_on"),
        F.col("st.gpa").alias("student_gpa"),
        F.col("e.score").alias("enrollment_score"),
        F.col("e.grade_status").alias("enrollment_grade_status_2"),
    )
)

result = (
    enriched_student
    .select(
        "id",
        "student_id",
        "enrollment_id",
        "enrollment_term",
        "days_since_enrolled_at_rank",
        "student_id_enrollment_grade_status_sum_days_sinc",
        "student_name",
        "student_enrolled_on",
        "student_gpa",
        "enrollment_grade_status_2",
        (F.col("days_since_enrolled_at") - F.col("student_gpa")).alias("days_since_enrolled_at_minus_student_gpa"),
        F.coalesce(F.col("enrollment_term"), F.lit("UNKNOWN")).alias("enrollment_term_or_unknown"),
    )
    .orderBy(
        F.col("student_name").asc_nulls_last(),
        F.col("student_enrolled_on").asc_nulls_last(),
        "id",
        "enrollment_id",
    )
    .limit(5)
)
