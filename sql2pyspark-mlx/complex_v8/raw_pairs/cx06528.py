from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_dedup = Window.partitionBy("student_id", "enrollment_term").orderBy("enrollment_score", "id", "enrollment_id")

filtered = (
    df_enrollments
    .filter((~F.col("grade_status").isin("FAIL", "INCOMPLETE")) | (F.col("course_id").isNotNull()))
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

enriched = (
    filtered
    .select(
        "id",
        "student_id",
        "course_id",
        "term",
        "score",
        "grade_status",
        F.coalesce(F.col("score"), F.lit(0.0)).alias("score_filled"),
        (F.floor(F.col("score") / 25) * 25).alias("score_bucket"),
        F.datediff(F.lit("2025-06-30").cast("date"), F.col("enrolled_at")).alias("days_since_enrolled_at"),
    )
)

joined = (
    enriched.alias("s")
    .join(df_students.alias("st"), F.col("st.id") == F.col("s.student_id"), "left")
    .select(
        "s.id",
        "s.student_id",
        "s.course_id",
        "s.term",
        "s.grade_status",
        "s.score_filled",
        "s.score_bucket",
        "s.days_since_enrolled_at",
        F.col("st.major").alias("student_major"),
    )
)

screened = (
    joined.alias("s")
    .join(df_enrollments.alias("e"), (F.col("e.student_id") == F.col("s.student_id")) & (F.col("e.course_id").isNotNull()) & (F.col("e.term") == "2025T2"), "left_anti")
)

line_level_enrollments = (
    screened.alias("s")
    .join(df_enrollments.alias("e"), F.col("e.student_id") == F.col("s.student_id"), "inner")
    .select(
        "s.id",
        "s.student_id",
        "s.course_id",
        "s.score_bucket",
        "s.days_since_enrolled_at",
        F.col("e.id").alias("enrollment_id"),
        F.col("e.term").alias("enrollment_term"),
        F.col("e.enrolled_at").alias("enrollment_enrolled_at"),
        F.col("e.score").alias("enrollment_score"),
    )
)

latest_per_key = (
    line_level_enrollments
    .withColumn("recency_rank", F.row_number().over(w_dedup))
    .filter(F.col("recency_rank") == 1)
    .drop("recency_rank")
)

enriched_student = (
    latest_per_key.alias("s")
    .join(df_students.alias("st"), F.col("st.id") == F.col("s.student_id"), "inner")
    .filter(F.col("st.gpa") <= 3.4)
    .select(
        "s.id",
        "s.student_id",
        "s.course_id",
        "s.score_bucket",
        "s.days_since_enrolled_at",
        "s.enrollment_id",
        "s.enrollment_term",
        "s.enrollment_score",
        F.col("st.gpa").alias("student_gpa"),
        F.col("st.is_international").alias("student_is_international"),
    )
)

result = (
    enriched_student
    .filter(F.col("student_id").isNotNull())
    .select(
        "id",
        "student_id",
        "course_id",
        "score_bucket",
        "days_since_enrolled_at",
        "enrollment_id",
        "enrollment_term",
        "enrollment_score",
        "student_gpa",
        F.lower(F.regexp_replace(F.trim(F.col("enrollment_term")), " ", "_")).alias("enrollment_term_slug"),
    )
    .orderBy(F.col("enrollment_term").asc(), F.col("enrollment_id").desc())
)
