from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("country").orderBy("event_time", "id")
w2 = Window.partitionBy("country").orderBy("event_time", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w3 = Window.partitionBy(F.to_date("prev_event_time"))
w4 = Window.partitionBy("session_country").orderBy(F.col("duration_sec").desc())
w_recent = Window.partitionBy("session_browser").orderBy(F.col("is_new_burst").desc())
w6 = Window.partitionBy("device").orderBy("session_id_2", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w7 = Window.partitionBy("session_country_2").orderBy("event_time", "id")

base = (
    df_web_events
    .filter(~F.col("page").eqNullSafe(F.lit("/search")))
    .select(
        "*",
        F.col("duration_sec").cast("double").alias("duration_sec_num"),
        F.coalesce("page", F.lit("UNKNOWN")).alias("page_clean"),
    )
)

with_dims = (
    base
    .alias("e")
    .join(df_sessions.alias("b"), F.col("e.session_id") == F.col("b.id"), "left")
    .select(
        "e.id",
        "e.session_id",
        "e.event_type",
        "e.page",
        "e.event_time",
        "e.duration_sec",
        "e.page_clean",
        F.col("b.browser"),
        F.col("b.device"),
        F.col("b.country"),
    )
)

with_prev = with_dims.withColumn("prev_event_time", F.lag("event_time").over(w))

flagged = (
    with_prev
    .withColumn("is_new_burst", F.when(F.col("prev_event_time").isNull() | (F.col("event_time") > F.col("prev_event_time") + F.expr("INTERVAL 7 DAYS")), 1).otherwise(0))
)

sessions_web_events = flagged.withColumn("session_id_2", F.sum("is_new_burst").over(w2))

with_dims_web_events = (
    sessions_web_events
    .alias("s")
    .join(df_sessions.alias("p"), F.col("s.session_id") == F.col("p.id"), "left")
    .filter(F.col("p.browser") == "Edge")
    .select("s.*", F.col("p.country").alias("session_country"))
)

joined = (
    with_dims_web_events
    .alias("e")
    .join(df_sessions.alias("d"), F.col("e.session_id") == F.col("d.id"), "left")
    .select(
        "e.*",
        F.col("d.browser").alias("session_browser"),
        F.col("d.country").alias("session_country_2"),
        F.col("d.device").alias("session_device"),
    )
)

metrics = (
    joined
    .select(
        "*",
        F.sum("session_id_2").over(w3).alias("prev_event_time_to_date_sum_session_id_2"),
        F.rank().over(w4).alias("duration_sec_rank"),
        F.rank().over(w_recent).alias("is_new_burst_rank"),
        F.first("session_device", ignorenulls=True).over(w6).alias("first_known_session_device"),
        F.lag("session_id_2", 1).over(w7).alias("prev_session_id_2"),
    )
)

cleaned = (
    metrics
    .filter((F.col("event_time") > F.lit("2023-06-24 17:00:00").cast("timestamp")) & (~F.col("browser").eqNullSafe(F.lit("Edge"))))
    .select(
        "*",
        F.round((F.col("is_new_burst_rank") * 0.9), 2).alias("adjusted_is_new_burst_rank"),
        F.coalesce("device", F.lit("DIRECT")).alias("device_filled"),
    )
)

result = (
    cleaned
    .groupBy(
        "session_browser",
        F.when(F.col("prev_session_id_2") >= 6, "HIGH").otherwise("LOW").alias("prev_session_id_2_tier"),
    )
    .agg(
        F.round(F.avg("session_id_2"), 2).alias("avg_session_id_2"),
        F.max("event_time").alias("last_seen"),
        F.countDistinct("device").alias("distinct_devices"),
    )
    .filter(F.col("distinct_devices") > 1)
    .orderBy(F.col("avg_session_id_2").asc_nulls_first())
)
