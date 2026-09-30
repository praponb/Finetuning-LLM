from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_term = Window.partitionBy("term").orderBy("enrolled_at", "id")
w_term_running = Window.partitionBy("term").orderBy("enrolled_at", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

recent = (
    df_enrollments
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

with_prev = recent.withColumn("prev_enrolled_at", F.lag("enrolled_at").over(w_term))

flagged = (
    with_prev
    .withColumn("is_new_session", F.when(F.col("prev_enrolled_at").isNull() | ((F.unix_timestamp("enrolled_at") - F.unix_timestamp("prev_enrolled_at")) / 60 > 45), 1).otherwise(0))
)

sessionized = flagged.withColumn("session_no", F.sum("is_new_session").over(w_term_running))

result = (
    sessionized
    .select(
        "id",
        "student_id",
        "term",
        "score",
        "enrolled_at",
        "session_no",
        F.concat(F.col("grade_status"), F.lit("#"), F.col("session_no").cast("string")).alias("grade_status_ref"),
        F.round((F.unix_timestamp(F.col("enrolled_at")) - F.unix_timestamp(F.col("prev_enrolled_at"))) / 60, 1).alias("minutes_prev_enrolled_at_to_enrolled_at"),
    )
    .orderBy(F.col("enrolled_at").desc(), F.col("id").asc_nulls_last())
)
