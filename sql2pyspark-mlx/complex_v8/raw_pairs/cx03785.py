from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_term = Window.partitionBy("term").orderBy("enrolled_at", "id")
w_term_running = Window.partitionBy("term").orderBy("enrolled_at", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

cleaned = (
    df_enrollments
    .select(
        "id",
        "student_id",
        "course_id",
        "term",
        "score",
        "grade_status",
        "enrolled_at",
        F.month(F.col("enrolled_at")).alias("enrolled_at_month"),
    )
)

with_prev = cleaned.withColumn("prev_enrolled_at", F.lag("enrolled_at").over(w_term))

flagged = (
    with_prev
    .withColumn("is_new_session", F.when(F.col("prev_enrolled_at").isNull() | ((F.unix_timestamp("enrolled_at") - F.unix_timestamp("prev_enrolled_at")) / 60 > 120), 1).otherwise(0))
)

sessionized = flagged.withColumn("burst_id", F.sum("is_new_session").over(w_term_running))

session_summary = (
    sessionized
    .groupBy("term", "burst_id")
    .agg(
        F.min("enrolled_at").alias("session_start"),
        F.max("enrolled_at").alias("session_end"),
        F.count("*").alias("n_events"),
        F.sum("enrolled_at_month").alias("session_enrolled_at_month"),
        F.round((F.unix_timestamp(F.max("enrolled_at")) - F.unix_timestamp(F.min("enrolled_at"))) / 60, 1).alias("duration_min"),
    )
)

result = (
    session_summary
    .filter(F.col("n_events") < 6)
    .select(
        "term",
        "burst_id",
        "session_start",
        "n_events",
        "session_enrolled_at_month",
        "duration_min",
        F.round(F.col("duration_min") * 1.1, 2).alias("duration_min_adj"),
        (F.floor(F.col("duration_min") / 50) * 50).alias("duration_min_bucket"),
    )
    .orderBy(F.col("duration_min").desc(), F.col("duration_min_bucket").desc())
)
