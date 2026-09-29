from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_part = Window.partitionBy("page").orderBy("event_time", "id").rowsBetween(-3, Window.currentRow)
w2 = Window.orderBy(F.col("event_time").cast("bigint")).rangeBetween(-604800, Window.currentRow)

with_windows = (
    df_web_events
    .withColumn("last4_avg_duration_sec", F.avg("duration_sec").over(w_part))
    .withColumn("last4_min_duration_sec", F.min("duration_sec").over(w_part))
    .withColumn("last4_count_duration_sec", F.count("duration_sec").over(w_part))
    .withColumn("duration_sec_avg_7d", F.avg("duration_sec").over(w2))
)

result = (
    with_windows
    .groupBy(
        "event_type",
        "session_id",
        F.date_trunc("MONTH", "event_time").alias("activity_month"),
    )
    .agg(
        F.max("event_time").alias("last_event_time"),
        F.bit_and("duration_sec").alias("duration_sec_bit_and"),
        ((F.unix_timestamp(F.max("event_time")) - F.unix_timestamp(F.min("event_time"))) / 60).alias("span_minutes"),
    )
)
