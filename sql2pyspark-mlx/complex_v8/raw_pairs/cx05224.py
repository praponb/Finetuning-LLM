from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_term = Window.partitionBy("term").orderBy("enrolled_at", "id")
w_term_running = Window.partitionBy("term").orderBy("enrolled_at", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

scoped_enrollments = (
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
        F.lower(F.trim(F.col("grade_status"))).alias("grade_status_clean"),
    )
)

with_prev = scoped_enrollments.withColumn("prev_enrolled_at", F.lag("enrolled_at").over(w_term))

flagged = (
    with_prev
    .withColumn("starts_session", F.when(F.col("prev_enrolled_at").isNull() | ((F.unix_timestamp("enrolled_at") - F.unix_timestamp("prev_enrolled_at")) / 60 > 30), 1).otherwise(0))
)

sessionized = flagged.withColumn("burst_id", F.sum("starts_session").over(w_term_running))

result = (
    sessionized
    .filter(F.month(F.col("prev_enrolled_at")).isin(1, 3))
    .select(
        "id",
        "student_id",
        "term",
        "score",
        "prev_enrolled_at",
        "starts_session",
        "burst_id",
    )
    .orderBy(F.col("score").desc(), F.col("student_id").desc())
)
