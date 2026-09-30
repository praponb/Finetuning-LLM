from pyspark.sql import functions as F

valid = (
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
    )
)

annotated = (
    valid
    .filter(F.col("grade_status") == "PASS")
    .select(
        "id",
        "student_id",
        "course_id",
        "term",
        "enrolled_at",
        F.date_trunc("week", F.col("enrolled_at")).alias("enrolled_at_week"),
        F.round(F.col("score") * 1.5, 2).alias("score_adj"),
        F.date_format(F.col("enrolled_at"), "yyyy-MM").alias("enrolled_at_label"),
    )
)

tagged = (
    annotated
    .filter(F.col("student_id") <= 19)
    .select(
        "id",
        "student_id",
        "enrolled_at_week",
        "score_adj",
        "enrolled_at_label",
        F.lower(F.regexp_replace(F.trim(F.col("enrolled_at_label")), " ", "_")).alias("enrolled_at_label_slug"),
    )
)

with_parent_student = (
    tagged.alias("s")
    .join(df_students.alias("st"), F.col("st.id") == F.col("s.student_id"), "inner")
    .select(
        "s.id",
        "s.student_id",
        "s.enrolled_at_week",
        "s.score_adj",
        "s.enrolled_at_label_slug",
        "st.student_name",
        F.col("st.major").alias("student_major"),
    )
)

result = (
    with_parent_student
    .filter(F.col("student_id").isNotNull())
    .select(
        "id",
        "enrolled_at_week",
        "score_adj",
        "enrolled_at_label_slug",
        "student_major",
        F.datediff(F.lit("2026-01-01").cast("date"), F.col("enrolled_at_week")).alias("days_since_enrolled_at_week"),
    )
    .orderBy(F.col("enrolled_at_week").desc(), F.col("score_adj").desc())
)
