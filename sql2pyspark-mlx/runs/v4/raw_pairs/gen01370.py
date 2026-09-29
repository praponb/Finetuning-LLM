from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("session_id").orderBy("event_time", "id")
w2 = Window.partitionBy("session_id").orderBy("event_time", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

filtered = (
    df_web_events
    .filter(F.col("event_time") > F.lit("2023-10-25").cast("date"))
    .withColumn("event_time_hour", F.hour("event_time"))
    .withColumn("page_filled", F.coalesce("page", F.lit("NONE")))
)

gaps = (
    filtered
    .withColumn("session_start_flag", F.when(F.lag("event_time").over(w).isNull() | (F.col("event_time") > F.lag("event_time").over(w) + F.expr("INTERVAL 90 DAYS")), 1).otherwise(0))
)

islands = gaps.withColumn("session_id_2", F.sum("session_start_flag").over(w2))

result = (
    islands
    .groupBy("event_type")
    .agg(
        F.sum("duration_sec").alias("total_duration_sec"),
        F.avg(F.when(F.col("page") == "/checkout", F.col("session_id_2"))).alias("checkout_avg_session_id_2"),
    )
    .filter(F.col("total_duration_sec") >= 0)
    .orderBy(F.col("total_duration_sec").asc_nulls_first())
)
