from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_latest = Window.partitionBy("grade_status").orderBy(F.col("enrolled_at").desc(), F.col("id").desc())

base_enrollments = (
    df_enrollments
    .filter((F.col("grade_status") == "WITHDRAWN") & (F.col("score") <= 83.1))
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

latest_per_key = (
    base_enrollments
    .withColumn("recency_rank", F.row_number().over(w_latest))
    .filter(F.col("recency_rank") == 1)
    .drop("recency_rank")
)

scored = (
    latest_per_key
    .select(
        "*",
        F.datediff(F.lit("2025-06-30").cast("date"), F.col("enrolled_at")).alias("days_since_enrolled_at"),
        F.concat(F.col("grade_status"), F.lit("#"), F.col("id").cast("string")).alias("grade_status_ref"),
    )
)

enriched = (
    scored.alias("s")
    .join(df_students.alias("st"), F.col("st.id") == F.col("s.student_id"), "inner")
    .filter(F.upper(F.trim(F.col("st.student_name"))).like("A%"))
    .select(
        "s.id",
        "s.student_id",
        "s.course_id",
        "s.term",
        "s.score",
        "s.grade_status",
        "s.enrolled_at",
        "s.days_since_enrolled_at",
        F.col("st.gpa").alias("student_gpa"),
        F.col("st.major").alias("student_major"),
        "st.student_name",
    )
)

enrollment_stats = (
    df_enrollments
    .groupBy("course_id")
    .agg(F.max("enrolled_at").alias("last_enrollment_enrolled_at"))
)

with_stats = (
    enriched.alias("s")
    .join(enrollment_stats.alias("e"), F.col("e.course_id") == F.col("s.course_id"), "left")
    .select("s.*", "e.last_enrollment_enrolled_at")
)

enriched_2 = (
    with_stats.alias("s")
    .join(df_students.alias("st"), F.col("st.id") == F.col("s.student_id"), "inner")
    .select(
        "s.id",
        "s.student_id",
        "s.course_id",
        "s.term",
        "s.score",
        "s.grade_status",
        "s.enrolled_at",
        "s.student_major",
        "s.last_enrollment_enrolled_at",
        F.col("st.cohort").alias("student_cohort"),
    )
)

result = (
    enriched_2
    .select(
        "id",
        "student_id",
        "course_id",
        "term",
        "grade_status",
        "enrolled_at",
        "student_major",
        "last_enrollment_enrolled_at",
    )
    .orderBy("id")
    .limit(10)
)
