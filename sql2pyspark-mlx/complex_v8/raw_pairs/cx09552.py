from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_course_id = Window.partitionBy("course_id").orderBy("enrolled_at", "id")
w_course_id_running = Window.partitionBy("course_id").orderBy("enrolled_at", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

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

with_prev = valid.withColumn("prev_enrolled_at", F.lag("enrolled_at").over(w_course_id))

flagged = (
    with_prev
    .withColumn("is_new_session", F.when(F.col("prev_enrolled_at").isNull() | (F.col("enrolled_at") > F.col("prev_enrolled_at") + F.expr("INTERVAL 45 MINUTES")), 1).otherwise(0))
)

sessionized = flagged.withColumn("session_no", F.sum("is_new_session").over(w_course_id_running))

result = (
    sessionized
    .select(
        "id",
        "student_id",
        "course_id",
        "term",
        "score",
        "prev_enrolled_at",
        F.coalesce(F.col("term"), F.lit("UNKNOWN")).alias("term_or_unknown"),
    )
    .orderBy("id")
    .limit(5)
)
