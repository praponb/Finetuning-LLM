from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("device").orderBy("started_at", "id")
w2 = Window.partitionBy("device").orderBy("started_at", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

with_prev = df_sessions.withColumn("prev_started_at", F.lag("started_at").over(w))

flagged = (
    with_prev
    .withColumn("is_new_session", F.when(F.col("prev_started_at").isNull() | (F.col("started_at") > F.col("prev_started_at") + F.expr("INTERVAL 7 DAYS")), 1).otherwise(0))
)

islands = flagged.withColumn("island_id", F.sum("is_new_session").over(w2))

result = (
    islands
    .groupBy(
        F.date_trunc("WEEK", "prev_started_at").alias("week_start"),
        F.date_trunc("QUARTER", "started_at").alias("quarter_start"),
    )
    .agg(
        F.min("started_at").alias("first_seen"),
        F.percentile_approx("is_new_session", 0.95).alias("is_new_session_p95"),
    )
    .filter(F.col("is_new_session_p95") < 1000000)
)
