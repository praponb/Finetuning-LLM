from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_term_or_unknown = Window.partitionBy("term_or_unknown").orderBy("enrolled_at", "id")
w_term_or_unknown_running = Window.partitionBy("term_or_unknown").orderBy("enrolled_at", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

prepared = (
    df_enrollments
    .filter(F.col("course_id") <= 6)
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

with_prev = prepared.withColumn("prev_enrolled_at", F.lag("enrolled_at").over(w_term_or_unknown))

flagged = (
    with_prev
    .withColumn("new_session_flag", F.when(F.col("prev_enrolled_at").isNull() | (F.col("enrolled_at") > F.col("prev_enrolled_at") + F.expr("INTERVAL 90 MINUTES")), 1).otherwise(0))
)

sessionized = flagged.withColumn("burst_id", F.sum("new_session_flag").over(w_term_or_unknown_running))

sessions = (
    sessionized
    .groupBy("term_or_unknown", "burst_id")
    .agg(
        F.min("enrolled_at").alias("session_start"),
        F.max("enrolled_at").alias("session_end"),
        F.count("*").alias("events"),
        F.round((F.unix_timestamp(F.max("enrolled_at")) - F.unix_timestamp(F.min("enrolled_at"))) / 60, 1).alias("duration_min"),
    )
)

labeled = (
    sessions
    .select(
        "*",
        F.initcap(F.trim(F.col("term_or_unknown"))).alias("term_or_unknown_clean"),
        F.round(F.col("burst_id") / F.nullif(F.col("duration_min"), F.lit(0)), 3).alias("burst_id_per_duration_min"),
    )
)

result = (
    labeled
    .filter(F.col("term_or_unknown_clean").isNotNull())
    .select(
        "term_or_unknown",
        "burst_id",
        "session_start",
        "session_end",
        "events",
        "duration_min",
        "term_or_unknown_clean",
        "burst_id_per_duration_min",
        F.year(F.col("session_start")).alias("session_start_year"),
        F.to_date(F.col("session_start")).alias("session_start_date"),
    )
)
