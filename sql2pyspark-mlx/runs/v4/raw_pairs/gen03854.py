from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("session_id").orderBy("duration_sec", "id")
w_hist = Window.orderBy(F.col("event_time").cast("bigint")).rangeBetween(-7776000, Window.currentRow)
w3 = Window.partitionBy("page").orderBy(F.col("duration_sec").desc())
w4 = Window.partitionBy("session_id").orderBy("event_time", "id")

joined = (
    df_web_events
    .alias("s")
    .join(df_sessions.alias("b"), F.col("s.session_id") == F.col("b.id"), "left")
    .select("s.*", F.col("b.device"), F.col("b.browser"), F.col("b.country"))
)

with_windows = (
    joined
    .withColumn("duration_sec_bucket", F.ntile(4).over(w))
    .withColumn("duration_sec_avg_90d", F.avg("duration_sec").over(w_hist))
    .withColumn("duration_sec_rank", F.rank().over(w3))
)

deduped = (
    with_windows
    .withColumn("dedup_rank", F.row_number().over(w4))
    .filter(F.col("dedup_rank") == 1)
)

result = (
    deduped
    .groupBy(F.date_trunc("year", "event_time").alias("year_start"))
    .agg(
        F.min("event_time").alias("started_at"),
        F.round(F.sum("duration_sec_rank") / F.nullif(F.count(F.when(F.col("page") == "/checkout", 1)), F.lit(0)), 2).alias("duration_sec_rank_per_checkout"),
        F.bit_or("duration_sec").alias("duration_sec_bit_or"),
    )
)
