from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_grade_status = Window.partitionBy("grade_status").orderBy("enrolled_at", "id")
w_grade_status_running = Window.partitionBy("grade_status").orderBy("enrolled_at", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

valid_enrollments = (
    df_enrollments
    .select(
        "id",
        "student_id",
        "course_id",
        "term",
        "score",
        "grade_status",
        "enrolled_at",
        F.round(F.col("score") * 1.1, 2).alias("score_adj"),
    )
)

with_prev = valid_enrollments.withColumn("prev_enrolled_at", F.lag("enrolled_at").over(w_grade_status))

flagged = (
    with_prev
    .withColumn("starts_session", F.when(F.col("prev_enrolled_at").isNull() | ((F.unix_timestamp("enrolled_at") - F.unix_timestamp("prev_enrolled_at")) / 60 > 120), 1).otherwise(0))
)

sessionized = flagged.withColumn("session_id", F.sum("starts_session").over(w_grade_status_running))

session_summary = (
    sessionized
    .groupBy("grade_status", "session_id")
    .agg(
        F.min("enrolled_at").alias("session_start"),
        F.max("enrolled_at").alias("session_end"),
        F.count("*").alias("n_events"),
        F.sum("score_adj").alias("session_score_adj"),
        F.round((F.unix_timestamp(F.max("enrolled_at")) - F.unix_timestamp(F.min("enrolled_at"))) / 60, 1).alias("duration_min"),
    )
)

enriched = (
    session_summary
    .filter(F.month(F.col("session_start")).isin(1, 2))
    .select(
        "*",
        F.concat(F.col("grade_status"), F.lit("#"), F.col("session_id").cast("string")).alias("grade_status_ref"),
    )
)

derived = (
    enriched
    .filter(F.hour(F.col("session_end")).between(10, 15))
    .select(
        "*",
        F.round(F.col("session_score_adj") / F.nullif(F.col("n_events"), F.lit(0)), 3).alias("session_score_adj_per_n_events"),
    )
)

result = (
    derived
    .select(
        "grade_status",
        "session_start",
        "session_end",
        "n_events",
        "session_score_adj",
        "duration_min",
        "grade_status_ref",
        F.round(F.col("session_score_adj") * 1.1, 2).alias("session_score_adj_adj"),
    )
)
