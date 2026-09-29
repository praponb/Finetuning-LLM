from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("country").orderBy("started_at", "id")
w2 = Window.partitionBy("country").orderBy("started_at", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

flagged = (
    df_sessions
    .withColumn("is_new_burst", F.when(F.lag("started_at").over(w).isNull() | (F.col("started_at") > F.lag("started_at").over(w) + F.expr("INTERVAL 7 DAYS")), 1).otherwise(0))
)

sessionized = flagged.withColumn("session_no", F.sum("is_new_burst").over(w2))

result = (
    sessionized
    .groupBy(F.date_trunc("MONTH", "started_at").alias("activity_month"))
    .agg(
        F.avg("session_no").alias("avg_session_no"),
        F.percentile_approx("session_no", [0.25, 0.5, 0.75]).alias("session_no_quartiles"),
        F.min("started_at").alias("first_started_at"),
        F.sum(F.when(F.col("device") == "desktop", F.col("session_no")).otherwise(0)).alias("desktop_session_no"),
        F.round(F.avg("is_new_burst"), 2).alias("avg_is_new_burst"),
    )
    .orderBy("avg_session_no")
)
