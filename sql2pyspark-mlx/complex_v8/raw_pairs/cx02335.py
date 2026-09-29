from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_course_id = Window.partitionBy("course_id").orderBy("enrolled_at", "id")
w_course_id_running = Window.partitionBy("course_id").orderBy("enrolled_at", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

recent_enrollments = (
    df_enrollments
    .filter(F.col("score") < 62.5)
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

with_prev = recent_enrollments.withColumn("prev_enrolled_at", F.lag("enrolled_at").over(w_course_id))

flagged = (
    with_prev
    .withColumn("new_session_flag", F.when(F.col("prev_enrolled_at").isNull() | ((F.unix_timestamp("enrolled_at") - F.unix_timestamp("prev_enrolled_at")) / 60 > 45), 1).otherwise(0))
)

sessionized = flagged.withColumn("session_id", F.sum("new_session_flag").over(w_course_id_running))

with_details_course = (
    sessionized.alias("s")
    .join(df_courses.alias("c"), F.col("c.id") == F.col("s.course_id"), "inner")
    .select(
        "s.id",
        "s.student_id",
        "s.course_id",
        "s.score",
        "s.grade_status",
        "s.new_session_flag",
        "s.session_id",
        F.col("c.credits").alias("course_credits"),
        "c.course_code",
        F.col("c.level").alias("course_level"),
    )
)

result = (
    with_details_course
    .select(
        "id",
        "course_id",
        "score",
        "new_session_flag",
        "session_id",
        F.when(F.col("new_session_flag") >= 1, "HIGH").when(F.col("new_session_flag") >= 0, "MEDIUM").otherwise("LOW").alias("new_session_flag_tier"),
    )
)
