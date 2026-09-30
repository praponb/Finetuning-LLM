from pyspark.sql import functions as F

recent = (
    df_enrollments
    .filter((F.to_date(F.col("enrolled_at")) < F.lit("2025-02-24").cast("date")) & (F.col("score") > 42.4))
    .select(
        "id",
        "student_id",
        "course_id",
        "term",
        "score",
        "grade_status",
        "enrolled_at",
        F.concat(F.col("term"), F.lit("#"), F.col("course_id").cast("string")).alias("term_ref"),
        F.round(F.col("score") * 1.5, 2).alias("score_adj"),
    )
)

joined = (
    recent.alias("s")
    .join(df_students.alias("st"), F.col("st.id") == F.col("s.student_id"), "inner")
    .select(
        "s.id",
        "s.student_id",
        "s.course_id",
        "s.score",
        "s.grade_status",
        "s.enrolled_at",
        "s.score_adj",
        F.col("st.major").alias("student_major"),
    )
)

result = (
    joined
    .groupBy("enrolled_at")
    .pivot("grade_status", ["WITHDRAWN", "INCOMPLETE"])
    .agg(F.avg("score"))
)
