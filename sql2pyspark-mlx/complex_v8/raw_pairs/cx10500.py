from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_term = Window.partitionBy("term").orderBy("enrolled_at", "id")
w_term_running = Window.partitionBy("term").orderBy("enrolled_at", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

base = (
    df_enrollments
    .select(
        "id",
        "student_id",
        "course_id",
        "term",
        "score",
        "grade_status",
        "enrolled_at",
        F.quarter(F.col("enrolled_at")).alias("enrolled_at_quarter"),
    )
)

with_prev = base.withColumn("prev_enrolled_at", F.lag("enrolled_at").over(w_term))

flagged = (
    with_prev
    .withColumn("starts_session", F.when(F.col("prev_enrolled_at").isNull() | (F.col("enrolled_at") > F.col("prev_enrolled_at") + F.expr("INTERVAL 90 MINUTES")), 1).otherwise(0))
)

sessionized = flagged.withColumn("session_id", F.sum("starts_session").over(w_term_running))

per_session = (
    sessionized
    .groupBy("term", "session_id")
    .agg(
        F.min("enrolled_at").alias("session_start"),
        F.max("enrolled_at").alias("session_end"),
        F.count("*").alias("event_count"),
        F.sum("score").alias("session_score"),
        F.round((F.unix_timestamp(F.max("enrolled_at")) - F.unix_timestamp(F.min("enrolled_at"))) / 60, 1).alias("duration_min"),
    )
)

annotated = (
    per_session
    .select(
        "term",
        "session_id",
        "event_count",
        "session_score",
        "duration_min",
        F.abs(F.col("session_id") - 3).alias("session_id_distance"),
        F.coalesce(F.col("term"), F.lit("UNKNOWN")).alias("term_or_unknown"),
        F.when(F.col("term").isin("2025T2", "2024T1"), "group_a").when(F.col("term") == "2024T2", "group_b").otherwise("other").alias("term_group"),
    )
)

result = (
    annotated
    .filter(F.col("term").isin("2024T1", "2025T1", "2024T2"))
    .select(
        "term",
        "event_count",
        "duration_min",
        "session_id_distance",
        "term_or_unknown",
        "term_group",
        F.lower(F.regexp_replace(F.trim(F.col("term_or_unknown")), " ", "_")).alias("term_or_unknown_slug"),
        F.upper(F.trim(F.col("term_or_unknown"))).alias("term_or_unknown_clean"),
    )
    .orderBy(F.col("duration_min").desc(), F.col("term_or_unknown_clean").desc())
)
