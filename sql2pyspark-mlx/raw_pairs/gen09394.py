from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("event_type").orderBy("event_time", "id")
w2 = Window.partitionBy("event_type").orderBy("event_time", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_ordered = Window.partitionBy("event_type").orderBy("island_id", "id")

filtered = (
    df_web_events
    .filter(F.col("event_time") >= F.lit("2023-08-31 17:00:00").cast("timestamp"))
    .withColumn("page_length", F.length("page"))
    .withColumn("page_clean", F.coalesce("page", F.lit("OTHER")))
)

flagged = (
    filtered
    .withColumn("is_new_burst", F.when(F.col("event_time") - F.lag("event_time").over(w) > F.expr("INTERVAL 30 DAYS"), 1).otherwise(0))
)

sessionized = flagged.withColumn("island_id", F.sum("is_new_burst").over(w2))

joined = (
    sessionized
    .alias("s")
    .join(df_sessions.alias("p"), F.col("s.session_id") == F.col("p.id"), "left")
    .filter(F.col("p.started_at") < "2025-03-01")
    .select(
        "s.id",
        "s.session_id",
        "s.event_type",
        "s.page_clean",
        "s.island_id",
        F.col("p.device"),
        F.col("p.country"),
    )
)

with_dims = (
    joined
    .alias("a")
    .join(df_sessions.alias("p"), F.col("a.session_id") == F.col("p.id"), "inner")
    .select("a.*", F.col("p.country").alias("session_country"), F.col("p.browser"))
)

scored = (
    with_dims
    .withColumn("prev_island_id", F.lag("island_id").over(w_ordered))
    .withColumn("event_seq", F.row_number().over(w_ordered))
)

result = (
    scored
    .groupBy("session_country")
    .agg(
        F.countDistinct("session_country").alias("distinct_session_countries"),
        F.count(F.when(F.col("device") == "tablet", 1)).alias("tablet_count"),
        F.percentile_approx("event_seq", 0.5).alias("event_seq_p50"),
    )
    .orderBy("distinct_session_countries")
)
