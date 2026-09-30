from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_term = Window.partitionBy("term").orderBy("enrolled_at", "id")
w_term_running = Window.partitionBy("term").orderBy("enrolled_at", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

recent_enrollments = (
    df_enrollments
    .filter(F.col("student_id") >= 12)
    .select(
        "id",
        "student_id",
        "course_id",
        "term",
        "score",
        "grade_status",
        "enrolled_at",
        F.coalesce(F.col("score"), F.lit(0.0)).alias("score_filled"),
    )
)

with_prev = recent_enrollments.withColumn("prev_enrolled_at", F.lag("enrolled_at").over(w_term))

flagged = (
    with_prev
    .withColumn("is_new_session", F.when(F.col("prev_enrolled_at").isNull() | (F.col("enrolled_at") > F.col("prev_enrolled_at") + F.expr("INTERVAL 60 MINUTES")), 1).otherwise(0))
)

sessionized = flagged.withColumn("burst_id", F.sum("is_new_session").over(w_term_running))

with_parent_student = (
    sessionized.alias("s")
    .join(df_students.alias("st"), (F.col("st.id") == F.col("s.student_id")) & (F.col("st.major").isin("Bio", "Math")), "inner")
    .select(
        "s.id",
        "s.student_id",
        "s.course_id",
        "s.term",
        "s.enrolled_at",
        "s.score_filled",
        "s.prev_enrolled_at",
        "s.is_new_session",
        "s.burst_id",
        "st.student_name",
        F.col("st.major").alias("student_major"),
    )
)

result = (
    with_parent_student
    .filter(F.col("course_id") <= 4)
    .select(
        "id",
        "student_id",
        "term",
        "enrolled_at",
        "score_filled",
        "is_new_session",
        "burst_id",
        "student_name",
        F.datediff(F.lit("2026-01-01").cast("date"), F.col("prev_enrolled_at")).alias("days_since_prev_enrolled_at"),
    )
)
