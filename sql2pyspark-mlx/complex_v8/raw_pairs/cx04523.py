from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_student_id = Window.partitionBy("student_id").orderBy("enrolled_at", "id")
w_student_id_running = Window.partitionBy("student_id").orderBy("enrolled_at", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

cleaned = (
    df_enrollments
    .select(
        "id",
        "student_id",
        "course_id",
        "score",
        "grade_status",
        "enrolled_at",
        F.substring(F.trim(F.col("grade_status")), 1, 3).alias("grade_status_prefix"),
    )
)

with_prev = cleaned.withColumn("prev_enrolled_at", F.lag("enrolled_at").over(w_student_id))

flagged = (
    with_prev
    .withColumn("is_new_session", F.when(F.col("prev_enrolled_at").isNull() | (F.col("enrolled_at") > F.col("prev_enrolled_at") + F.expr("INTERVAL 30 MINUTES")), 1).otherwise(0))
)

sessionized = flagged.withColumn("session_id", F.sum("is_new_session").over(w_student_id_running))

combined_details_course = (
    sessionized.alias("s")
    .join(df_courses.alias("c"), F.col("c.id") == F.col("s.course_id"), "inner")
    .join(df_students.alias("st"), F.col("st.id") == F.col("s.student_id"), "left")
    .select(
        "s.id",
        "s.student_id",
        "s.course_id",
        "s.score",
        "s.prev_enrolled_at",
        "s.is_new_session",
        "s.session_id",
        F.col("c.level").alias("course_level"),
        F.col("c.credits").alias("course_credits"),
        F.col("st.major").alias("student_major"),
    )
)

result = (
    combined_details_course
    .select(
        "id",
        "student_id",
        "course_id",
        "score",
        "is_new_session",
        "session_id",
        "course_level",
        "course_credits",
        "student_major",
    )
    .orderBy(F.col("is_new_session").asc_nulls_last())
)
