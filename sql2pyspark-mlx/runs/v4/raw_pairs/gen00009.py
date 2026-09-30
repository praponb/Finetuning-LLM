from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("device").orderBy("started_at", "id")
w2 = Window.partitionBy("device").orderBy("started_at", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

with_prev = df_sessions.withColumn("prev_started_at", F.lag("started_at").over(w))

flagged = (
    with_prev
    .withColumn("session_start_flag", F.when(F.col("prev_started_at").isNull() | (F.col("started_at") > F.col("prev_started_at") + F.expr("INTERVAL 90 DAYS")), 1).otherwise(0))
)

sessions_sessions = flagged.withColumn("session_no", F.sum("session_start_flag").over(w2))

result = (
    sessions_sessions
    .groupBy(
        F.when(F.col("session_no") >= 7, "LARGE").otherwise("SMALL").alias("session_no_tier"),
    )
    .agg(
        F.percentile_approx("session_start_flag", 0.5).alias("session_start_flag_p50"),
        F.max("started_at").alias("last_seen"),
        F.percentile_approx("session_start_flag", [0.25, 0.5, 0.75]).alias("session_start_flag_quartiles"),
    )
)
