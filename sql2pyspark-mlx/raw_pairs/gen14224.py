from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("page", "event_type").orderBy(F.col("duration_sec").desc(), "id")
w2 = Window.partitionBy("browser").orderBy("event_time", "id")
w3 = Window.partitionBy("browser").orderBy("event_time", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w4 = Window.partitionBy("session_id", "event_type").orderBy("is_new_session", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w5 = Window.partitionBy(F.year("event_time")).orderBy(F.col("event_time").cast("bigint")).rangeBetween(-7776000, Window.currentRow)
w_recent = Window.partitionBy("page").orderBy("is_new_session", "id")
w7 = Window.partitionBy("session_id").orderBy("island_id_count_90d", "id").rowsBetween(-5, -1)
duration_sec_by_session_id = df_web_events.groupBy("session_id").agg(F.avg("duration_sec").alias("avg_web_event_duration_sec"))

ranked = (
    df_web_events
    .withColumn("latest_rank", F.row_number().over(w))
    .filter(F.col("latest_rank") == 1)
    .drop("latest_rank")
)

with_dims = (
    ranked
    .alias("a")
    .join(df_sessions.alias("s"), F.col("a.session_id") == F.col("s.id"), "inner")
    .filter(F.col("s.country") == "US")
    .select(
        "a.id",
        "a.session_id",
        "a.event_type",
        "a.page",
        "a.event_time",
        "a.duration_sec",
        F.col("s.browser"),
    )
)

gaps = (
    with_dims
    .withColumn("is_new_session", F.when(F.lag("event_time").over(w2).isNull() | (F.col("event_time") > F.lag("event_time").over(w2) + F.expr("INTERVAL 30 DAYS")), 1).otherwise(0))
)

sessionized = gaps.withColumn("island_id", F.sum("is_new_session").over(w3))

metrics = (
    sessionized
    .select(
        "*",
        F.first("browser", ignorenulls=True).over(w4).alias("first_known_browser"),
        F.count("island_id").over(w5).alias("island_id_count_90d"),
    )
)

metrics_web_events = (
    metrics
    .withColumn("next_duration_sec", F.lead("duration_sec").over(w_recent))
    .withColumn("next_island_id_count_90d", F.lead("island_id_count_90d").over(w_recent))
    .withColumn("event_seq", F.row_number().over(w_recent))
    .withColumn("duration_sec_avg_5_prior", F.avg("duration_sec").over(w7))
)

filtered = (
    metrics_web_events
    .filter(F.col("island_id") > 4)
    .select(
        "id",
        "session_id",
        "event_type",
        "page",
        "event_time",
        "duration_sec",
        "browser",
        "is_new_session",
        "island_id",
        "first_known_browser",
        "event_seq",
        "duration_sec_avg_5_prior",
        F.when(F.col("duration_sec") >= 441, "High").when(F.col("duration_sec") >= 223, "Medium").otherwise("Low").alias("duration_sec_tier"),
    )
)

result = (
    filtered
    .join(duration_sec_by_session_id, on="session_id", how="left")
    .select(
        "id",
        "session_id",
        "event_type",
        "page",
        "browser",
        "is_new_session",
        "duration_sec_tier",
        F.col("avg_web_event_duration_sec"),
    )
    .orderBy(F.col("is_new_session").desc())
)
