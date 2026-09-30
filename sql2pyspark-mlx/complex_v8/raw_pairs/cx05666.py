from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_term_student_is_internationa = Window.partitionBy("term", "student_is_international").orderBy(F.col("enrolled_at").desc_nulls_first(), "id")

cleaned_enrollments = (
    df_enrollments
    .filter((F.col("student_id") <= 19) | (F.col("course_id") <= 3))
    .select(
        "id",
        "student_id",
        "course_id",
        "term",
        "grade_status",
        "enrolled_at",
        F.lower(F.regexp_replace(F.trim(F.col("grade_status")), " ", "_")).alias("grade_status_slug"),
        F.when(F.col("score") >= 81.4, "HIGH").when(F.col("score") >= 42.4, "MEDIUM").otherwise("LOW").alias("score_tier"),
    )
)

with_details = (
    cleaned_enrollments.alias("s")
    .join(df_students.alias("st"), F.col("st.id") == F.col("s.student_id"), "left")
    .select(
        "s.id",
        "s.student_id",
        "s.course_id",
        "s.term",
        "s.grade_status",
        "s.enrolled_at",
        "s.grade_status_slug",
        "s.score_tier",
        F.col("st.is_international").alias("student_is_international"),
    )
)

ranked = (
    with_details
    .select(
        "id",
        "enrolled_at",
        "score_tier",
        "student_is_international",
        F.round((F.unix_timestamp("enrolled_at") - F.unix_timestamp(F.lag("enrolled_at").over(w_term_student_is_internationa))) / 60, 1).alias("minutes_since_prev_enrolled_at"),
        F.rank().over(w_term_student_is_internationa).alias("enrolled_at_rank"),
    )
)

result = (
    ranked
    .groupBy("enrolled_at")
    .pivot("score_tier", ["LOW", "MEDIUM"])
    .agg(F.avg("minutes_since_prev_enrolled_at"))
    .orderBy("enrolled_at")
)
