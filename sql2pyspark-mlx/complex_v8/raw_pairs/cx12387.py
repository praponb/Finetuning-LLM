from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_term = Window.partitionBy("term").orderBy("enrolled_at", "id")
w_term_running = Window.partitionBy("term").orderBy("enrolled_at", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

base_enrollments = df_enrollments

with_prev = base_enrollments.withColumn("prev_enrolled_at", F.lag("enrolled_at").over(w_term))

flagged = (
    with_prev
    .withColumn("new_session_flag", F.when(F.col("prev_enrolled_at").isNull() | (F.col("enrolled_at") > F.col("prev_enrolled_at") + F.expr("INTERVAL 30 MINUTES")), 1).otherwise(0))
)

sessionized = flagged.withColumn("session_id", F.sum("new_session_flag").over(w_term_running))

result = (
    sessionized
    .select(
        "id",
        "course_id",
        "score",
        "grade_status",
        "enrolled_at",
        "prev_enrolled_at",
        "new_session_flag",
        "session_id",
        F.when(F.col("session_id") >= 4, "HIGH").when(F.col("session_id") >= 3, "MEDIUM").otherwise("LOW").alias("session_id_tier"),
    )
)
