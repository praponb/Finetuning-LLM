from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("session_id").orderBy("event_time", "id")
w2 = Window.partitionBy("session_id").orderBy("event_time", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w3 = Window.partitionBy("page").orderBy(F.col("duration_sec").desc(), "id")
w4 = Window.partitionBy("page", "event_type").orderBy("island_id", "id")
w5 = Window.partitionBy("page").orderBy("duration_sec", "id")
w6 = Window.partitionBy(F.date_trunc("YEAR", "event_time"), "country")
w7 = Window.partitionBy("page").orderBy("duration_sec", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w8 = Window.partitionBy("page").orderBy(F.col("island_id").desc(), "id")
w_hist = Window.partitionBy("country").orderBy("event_time_period_sum_duration_sec", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w10 = Window.partitionBy("country", "session_id").orderBy(F.col("event_time").cast("bigint")).rangeBetween(-604800, Window.currentRow)
w_grp = Window.partitionBy("session_id").orderBy("is_new_session", "id")
w12 = Window.orderBy(F.col("event_time").cast("bigint")).rangeBetween(-7776000, Window.currentRow)

valid = (
    df_web_events
    .filter((F.col("event_time") >= "2025-01-28") & (F.col("event_time") > F.lit("2023-04-25").cast("date")))
    .withColumn("page_event_type_key", F.concat_ws("-", "page", "event_type"))
    .withColumn("page_filled", F.coalesce("page", F.lit("N/A")))
)

flagged = (
    valid
    .withColumn("is_new_session", F.when(F.lag("event_time").over(w).isNull() | (F.col("event_time") > F.lag("event_time").over(w) + F.expr("INTERVAL 60 DAYS")), 1).otherwise(0))
)

sessionized = flagged.withColumn("island_id", F.sum("is_new_session").over(w2))

with_dims = (
    sessionized
    .alias("a")
    .join(df_sessions.alias("s"), F.col("a.session_id") == F.col("s.id"), "left")
    .select("a.*", F.col("s.country"))
)

scored = (
    with_dims
    .select(
        "*",
        F.ntile(4).over(w3).alias("duration_sec_bucket"),
        F.row_number().over(w4).alias("seq_no"),
        F.row_number().over(w5).alias("seq_no_2"),
        F.sum("duration_sec").over(w6).alias("event_time_period_sum_duration_sec"),
        F.sum("is_new_session").over(w7).alias("cumulative_is_new_session"),
    )
)

result = (
    scored
    .withColumn("row_seq", F.row_number().over(w8))
    .withColumn("first_known_event_time_period_sum_duration_sec", F.first("event_time_period_sum_duration_sec", ignorenulls=True).over(w_hist))
    .withColumn("island_id_sum_7d", F.sum("island_id").over(w10))
    .withColumn("next_seq_no_2", F.lead("seq_no_2", 1, 0).over(w_grp))
    .withColumn("seq_no_2_avg_90d", F.avg("seq_no_2").over(w12))
)
