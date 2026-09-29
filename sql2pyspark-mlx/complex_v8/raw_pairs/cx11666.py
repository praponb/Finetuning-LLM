from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_term = Window.partitionBy("term").orderBy("enrolled_at", "id")
w_term_running = Window.partitionBy("term").orderBy("enrolled_at", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

prepared_enrollments = (
    df_enrollments
    .filter(F.hour(F.col("enrolled_at")).between(10, 14))
    .select(
        "id",
        "student_id",
        "course_id",
        "term",
        "score",
        "enrolled_at",
    )
)

with_prev = prepared_enrollments.withColumn("prev_enrolled_at", F.lag("enrolled_at").over(w_term))

flagged = (
    with_prev
    .withColumn("starts_session", F.when(F.col("prev_enrolled_at").isNull() | (F.col("enrolled_at") > F.col("prev_enrolled_at") + F.expr("INTERVAL 60 MINUTES")), 1).otherwise(0))
)

sessionized = flagged.withColumn("session_no", F.sum("starts_session").over(w_term_running))

with_details = (
    sessionized.alias("s")
    .join(df_students.alias("st"), F.col("st.id") == F.col("s.student_id"), "inner")
    .select(
        "s.id",
        "s.student_id",
        "s.course_id",
        "s.score",
        "s.enrolled_at",
        "s.prev_enrolled_at",
        "s.session_no",
        F.col("st.gpa").alias("student_gpa"),
        F.col("st.cohort").alias("student_cohort"),
        F.col("st.enrolled_on").alias("student_enrolled_on"),
    )
)

result = (
    with_details
    .groupBy("student_enrolled_on", "session_no")
    .pivot("student_cohort", ["C2021", "C2023", "C2022"])
    .agg(F.count("student_gpa").alias("count_v"), F.sum("student_gpa").alias("sum_v"))
    .orderBy("student_enrolled_on", "session_no")
)
