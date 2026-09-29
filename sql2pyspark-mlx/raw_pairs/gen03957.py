from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("device").orderBy("started_at", "id")
w2 = Window.partitionBy("device").orderBy("started_at", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

with_prev = df_sessions.withColumn("prev_started_at", F.lag("started_at").over(w))

with_gaps = (
    with_prev
    .withColumn("is_new_burst", F.when(F.col("prev_started_at").isNull() | (F.col("started_at") > F.col("prev_started_at") + F.expr("INTERVAL 60 DAYS")), 1).otherwise(0))
)

sessionized = (
    with_gaps
    .select(
        "id",
        "customer_id",
        "device",
        "browser",
        "started_at",
        "country",
        F.sum("is_new_burst").over(w2).alias("session_id"),
    )
)

result = (
    sessionized
    .groupBy("device", F.date_trunc("month", "started_at").alias("activity_month"))
    .agg(
        F.round(F.sum("session_id") / F.nullif(F.count(F.when(F.col("country") == "TH", 1)), F.lit(0)), 2).alias("session_id_per_th"),
        F.round(F.sum("session_id") / F.nullif(F.count(F.when(F.col("device") == "desktop", 1)), F.lit(0)), 2).alias("session_id_per_desktop"),
        F.min("started_at").alias("started_at"),
        F.count("*").alias("cnt"),
        F.min("session_id").alias("min_session_id"),
    )
    .orderBy(F.col("activity_month").desc())
)
