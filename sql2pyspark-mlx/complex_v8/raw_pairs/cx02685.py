from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_term = Window.partitionBy("term").orderBy("enrolled_at", "id")
w_term_running = Window.partitionBy("term").orderBy("enrolled_at", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

valid = (
    df_enrollments
    .select(
        "id",
        "student_id",
        "course_id",
        "term",
        "score",
        "enrolled_at",
    )
)

with_prev = valid.withColumn("prev_enrolled_at", F.lag("enrolled_at").over(w_term))

flagged = (
    with_prev
    .withColumn("starts_session", F.when(F.col("prev_enrolled_at").isNull() | (F.col("enrolled_at") > F.col("prev_enrolled_at") + F.expr("INTERVAL 60 MINUTES")), 1).otherwise(0))
)

sessionized = flagged.withColumn("burst_id", F.sum("starts_session").over(w_term_running))

combined_details = (
    sessionized.alias("s")
    .join(df_courses.alias("c"), F.col("c.id") == F.col("s.course_id"), "left")
    .select(
        "s.id",
        "s.student_id",
        "s.course_id",
        "s.enrolled_at",
        "s.prev_enrolled_at",
        "s.starts_session",
        "c.course_code",
        F.col("c.credits").alias("course_credits"),
        F.col("c.level").alias("course_level"),
    )
)

result = (
    combined_details
    .select(
        "id",
        "course_id",
        "enrolled_at",
        "prev_enrolled_at",
        "starts_session",
        "course_code",
        "course_level",
    )
    .orderBy(F.col("id").asc())
    .limit(10)
)
