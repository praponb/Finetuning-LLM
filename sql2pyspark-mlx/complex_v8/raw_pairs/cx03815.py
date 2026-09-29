from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_term_or_unknown = Window.partitionBy("term_or_unknown").orderBy("enrolled_at", "id")
w_term_or_unknown_running = Window.partitionBy("term_or_unknown").orderBy("enrolled_at", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

base_enrollments = (
    df_enrollments
    .select(
        "id",
        "student_id",
        "course_id",
        "term",
        "score",
        "grade_status",
        "enrolled_at",
        F.coalesce(F.col("term"), F.lit("UNKNOWN")).alias("term_or_unknown"),
    )
)

with_prev = base_enrollments.withColumn("prev_enrolled_at", F.lag("enrolled_at").over(w_term_or_unknown))

flagged = (
    with_prev
    .withColumn("is_new_session", F.when(F.col("prev_enrolled_at").isNull() | ((F.unix_timestamp("enrolled_at") - F.unix_timestamp("prev_enrolled_at")) / 60 > 60), 1).otherwise(0))
)

sessionized = flagged.withColumn("session_id", F.sum("is_new_session").over(w_term_or_unknown_running))

per_session = (
    sessionized
    .groupBy("term_or_unknown", "session_id")
    .agg(
        F.min("enrolled_at").alias("session_start"),
        F.max("enrolled_at").alias("session_end"),
        F.count("*").alias("events"),
        F.sum("score").alias("session_score"),
        F.round((F.unix_timestamp(F.max("enrolled_at")) - F.unix_timestamp(F.min("enrolled_at"))) / 60, 1).alias("duration_min"),
    )
)

result = (
    per_session
    .select(
        "term_or_unknown",
        "session_id",
        F.col("duration_min").alias("duration_min_v"),
        F.col("session_score").alias("m_session_score"),
    )
    .unpivot(
        ids=["term_or_unknown", "session_id"],
        values=["duration_min_v", "m_session_score"],
        variableColumnName="field",
        valueColumnName="reading",
    )
)
