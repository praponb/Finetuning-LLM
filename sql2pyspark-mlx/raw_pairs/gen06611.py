from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("session_id").orderBy("event_time", "id")
w2 = Window.partitionBy("session_id").orderBy("event_time", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

valid = (
    df_web_events
    .filter((F.col("event_time") > F.lit("2023-03-19 05:00:00").cast("timestamp")) & F.col("page").isNotNull())
    .select("*", F.to_date("event_time").alias("event_time_date"))
)

with_prev = valid.withColumn("prev_event_time", F.lag("event_time").over(w))

with_gaps = (
    with_prev
    .withColumn("is_new_session", F.when(F.col("prev_event_time").isNull() | (F.col("event_time") > F.col("prev_event_time") + F.expr("INTERVAL 7 DAYS")), 1).otherwise(0))
)

islands = with_gaps.withColumn("session_no", F.sum("is_new_session").over(w2))

result = (
    islands
    .groupBy(F.date_trunc("month", "prev_event_time").alias("activity_month"))
    .agg(
        F.min("session_no").alias("min_session_no"),
        F.round(F.avg("is_new_session"), 2).alias("avg_is_new_session"),
        F.bit_and("duration_sec").alias("duration_sec_bit_and"),
    )
    .orderBy(F.col("activity_month").asc_nulls_first())
)
