from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("country").orderBy("started_at", "id")
w2 = Window.partitionBy("country").orderBy("started_at", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

with_prev = df_sessions.withColumn("prev_started_at", F.lag("started_at").over(w))

flagged = (
    with_prev
    .withColumn("is_new_burst", F.when(F.col("prev_started_at").isNull() | (F.col("started_at") > F.col("prev_started_at") + F.expr("INTERVAL 30 DAYS")), 1).otherwise(0))
)

sessions_sessions = flagged.withColumn("island_id", F.sum("is_new_burst").over(w2))

result = (
    sessions_sessions
    .groupBy("country")
    .agg(
        F.count_if(F.col("browser") == "Safari").alias("safari_events"),
        F.min("prev_started_at").alias("started_at"),
    )
)
