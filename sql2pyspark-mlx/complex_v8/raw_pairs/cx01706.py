from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_all_by_score = Window.orderBy("score", "id")
w_all_by_score_ties = Window.orderBy("score")
w_course_id = Window.partitionBy("course_id")

prepared = (
    df_enrollments
    .filter(F.col("term") != "2025T1")
    .select(
        "id",
        "student_id",
        "course_id",
        "term",
        "score",
        "enrolled_at",
        F.date_trunc("quarter", F.col("enrolled_at")).alias("enrolled_at_quarter"),
        F.year(F.col("enrolled_at")).alias("enrolled_at_year"),
        F.concat(F.col("term"), F.lit("#"), F.col("id").cast("string")).alias("term_ref"),
    )
)

with_windows = (
    prepared
    .select(
        "*",
        F.round(F.cume_dist().over(w_all_by_score), 3).alias("score_cume_dist"),
        F.round(F.cume_dist().over(w_all_by_score_ties), 3).alias("score_cume_dist_2"),
        F.min("enrolled_at_year").over(w_course_id).alias("course_id_min_enrolled_at_year"),
    )
)

enriched = (
    with_windows
    .select(
        "id",
        "student_id",
        "score",
        "enrolled_at_quarter",
        "term_ref",
        "score_cume_dist",
        "score_cume_dist_2",
        F.datediff(F.lit("2025-12-31").cast("date"), F.col("enrolled_at")).alias("days_since_enrolled_at"),
        F.coalesce(F.col("score_cume_dist_2"), F.lit(0.0)).alias("score_cume_dist_2_filled"),
        F.lower(F.regexp_replace(F.trim(F.col("term")), " ", "_")).alias("term_slug"),
    )
)

line_level = (
    enriched.alias("s")
    .join(df_enrollments.alias("e"), F.col("e.student_id") == F.col("s.student_id"), "left")
    .select(
        "s.id",
        "s.student_id",
        "s.score",
        "s.enrolled_at_quarter",
        "s.term_ref",
        "s.days_since_enrolled_at",
        "s.term_slug",
        F.col("e.id").alias("enrollment_id"),
        F.col("e.score").alias("enrollment_score"),
        F.col("e.term").alias("enrollment_term"),
    )
)

enriched_enrollment = (
    line_level.alias("s")
    .join(df_enrollments.alias("e"), (F.col("e.id") == F.col("s.enrollment_id")) & (F.col("e.enrolled_at") < F.lit("2025-03-02 09:00:00").cast("timestamp")), "inner")
    .join(df_students.alias("st"), F.col("st.id") == F.col("e.student_id"), "left")
    .select(
        "s.id",
        "s.student_id",
        "s.score",
        "s.enrolled_at_quarter",
        "s.term_ref",
        "s.days_since_enrolled_at",
        "s.term_slug",
        "s.enrollment_id",
        "s.enrollment_term",
        F.col("e.student_id").alias("enrollment_student_id"),
        F.col("e.grade_status").alias("enrollment_grade_status"),
        F.col("st.cohort").alias("student_cohort"),
        F.col("st.major").alias("student_major"),
    )
)

result = (
    enriched_enrollment
    .filter(F.col("days_since_enrolled_at") >= 245)
    .select(
        "id",
        "student_id",
        "enrolled_at_quarter",
        "term_ref",
        "days_since_enrolled_at",
        "term_slug",
        "enrollment_id",
        "enrollment_term",
        "enrollment_student_id",
        "enrollment_grade_status",
        "student_cohort",
        "student_major",
        F.to_date(F.col("enrolled_at_quarter")).alias("enrolled_at_quarter_date"),
        F.round(F.col("days_since_enrolled_at") / F.nullif(F.col("score"), F.lit(0)), 3).alias("days_since_enrolled_at_per_score"),
    )
    .orderBy("id", "days_since_enrolled_at")
)
