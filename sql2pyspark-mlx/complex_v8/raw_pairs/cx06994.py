from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_course_id = Window.partitionBy("course_id").orderBy("enrolled_at", "id")
w_course_id_running = Window.partitionBy("course_id").orderBy("enrolled_at", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

filtered_enrollments = (
    df_enrollments
    .filter(F.col("score") < 90.9)
    .select(
        "id",
        "student_id",
        "course_id",
        "term",
        "score",
        "enrolled_at",
    )
)

with_prev = filtered_enrollments.withColumn("prev_enrolled_at", F.lag("enrolled_at").over(w_course_id))

flagged = (
    with_prev
    .withColumn("is_new_session", F.when(F.col("prev_enrolled_at").isNull() | (F.col("enrolled_at") > F.col("prev_enrolled_at") + F.expr("INTERVAL 30 MINUTES")), 1).otherwise(0))
)

sessionized = flagged.withColumn("session_no", F.sum("is_new_session").over(w_course_id_running))

exploded_detail = (
    sessionized.alias("s")
    .join(df_enrollments.alias("e"), F.col("e.course_id") == F.col("s.course_id"), "inner")
    .select(
        "s.id",
        "s.student_id",
        "s.course_id",
        "s.term",
        "s.enrolled_at",
        "s.is_new_session",
        "s.session_no",
        F.col("e.id").alias("enrollment_id"),
        F.col("e.enrolled_at").alias("enrollment_enrolled_at"),
        F.col("e.grade_status").alias("enrollment_grade_status"),
        F.col("e.student_id").alias("enrollment_student_id"),
        F.col("e.score").alias("enrollment_score"),
    )
)

result = (
    exploded_detail
    .filter(F.col("enrollment_id").isNotNull())
    .select(
        "id",
        "student_id",
        "course_id",
        "term",
        "enrolled_at",
        "is_new_session",
        "session_no",
        "enrollment_id",
        "enrollment_enrolled_at",
        "enrollment_grade_status",
        "enrollment_student_id",
    )
    .orderBy(
        F.col("enrollment_student_id").desc(),
        F.col("enrollment_enrolled_at").asc(),
        "id",
        "enrollment_id",
    )
    .limit(20)
)
