from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("device").orderBy("started_at", "id")
w2 = Window.partitionBy("device").orderBy("started_at", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w3 = Window.partitionBy("session_no").orderBy(F.col("session_no_per_tablet").desc())

prev_events = df_sessions.withColumn("prev_started_at", F.lag("started_at").over(w))

gaps = (
    prev_events
    .withColumn("is_new_burst", F.when(F.col("prev_started_at").isNull() | (F.col("started_at") > F.col("prev_started_at") + F.expr("INTERVAL 7 DAYS")), 1).otherwise(0))
)

sessionized = (
    gaps
    .select(
        "id",
        "customer_id",
        "device",
        "started_at",
        "country",
        F.sum("is_new_burst").over(w2).alias("session_no"),
    )
)

result = (
    sessionized
    .groupBy("session_no", F.date_trunc("WEEK", "started_at").alias("week_start"))
    .agg(
        F.round(F.sum("session_no") / F.nullif(F.count(F.when(F.col("device") == "tablet", 1)), F.lit(0)), 2).alias("session_no_per_tablet"),
        F.max("started_at").alias("last_started_at"),
        F.sum(F.when(F.col("device") == "desktop", F.col("session_no")).otherwise(0)).alias("desktop_session_no"),
    )
    .withColumn("session_no_per_tablet_percent_rank", F.percent_rank().over(w3))
)
