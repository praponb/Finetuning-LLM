from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_grade_status = Window.partitionBy("grade_status").orderBy("enrolled_at", "id")
w_grade_status_running = Window.partitionBy("grade_status").orderBy("enrolled_at", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

scoped = df_enrollments.filter(F.col("term").isin("2025T1", "2024T1", "2024T2"))

with_prev = scoped.withColumn("prev_enrolled_at", F.lag("enrolled_at").over(w_grade_status))

flagged = (
    with_prev
    .withColumn("new_session_flag", F.when(F.col("prev_enrolled_at").isNull() | ((F.unix_timestamp("enrolled_at") - F.unix_timestamp("prev_enrolled_at")) / 60 > 45), 1).otherwise(0))
)

sessionized = flagged.withColumn("burst_id", F.sum("new_session_flag").over(w_grade_status_running))

sessions = (
    sessionized
    .groupBy("grade_status", "burst_id")
    .agg(
        F.min("enrolled_at").alias("session_start"),
        F.max("enrolled_at").alias("session_end"),
        F.count("*").alias("event_count"),
        F.round((F.unix_timestamp(F.max("enrolled_at")) - F.unix_timestamp(F.min("enrolled_at"))) / 60, 1).alias("duration_min"),
    )
)

derived = (
    sessions
    .filter(F.hour(F.col("session_start")).between(10, 15))
    .select(
        "*",
        F.coalesce(F.col("burst_id"), F.lit(0)).alias("burst_id_filled"),
        F.substring(F.trim(F.col("grade_status")), 1, 3).alias("grade_status_prefix"),
    )
)

result = (
    derived
    .select(
        "grade_status",
        "burst_id",
        "session_end",
        "event_count",
        "duration_min",
        "burst_id_filled",
        "grade_status_prefix",
        F.datediff(F.lit("2025-12-31").cast("date"), F.col("session_start")).alias("days_since_session_start"),
    )
    .orderBy(F.col("grade_status_prefix").asc())
)
