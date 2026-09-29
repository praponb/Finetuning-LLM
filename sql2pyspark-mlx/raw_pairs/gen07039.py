from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("session_id").orderBy("event_time", "id")
w2 = Window.partitionBy("session_id").orderBy("event_time", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

flagged = (
    df_web_events
    .withColumn("is_new_session", F.when(F.lag("event_time").over(w).isNull() | (F.col("event_time") > F.lag("event_time").over(w) + F.expr("INTERVAL 7 DAYS")), 1).otherwise(0))
)

islands = (
    flagged
    .select(
        "id",
        "session_id",
        "event_time",
        "duration_sec",
        F.sum("is_new_session").over(w2).alias("burst_id"),
    )
)

result = (
    islands
    .withColumn("year_start", F.date_trunc("YEAR", "event_time"))
    .cube("burst_id", "year_start")
    .agg(
        F.max("event_time").alias("ended_at"),
        F.percentile_approx("duration_sec", 0.9).alias("duration_sec_p90"),
        F.grouping_id().alias("grouping_id"),
    )
)
