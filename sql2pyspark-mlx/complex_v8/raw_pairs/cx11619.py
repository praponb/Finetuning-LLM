from pyspark.sql import functions as F

prepared = (
    df_plays
    .filter((F.col("device") != "desktop") & (F.col("ms_played") > 112786))
    .select(
        "id",
        "listener_id",
        "track_id",
        "played_at",
        "ms_played",
        "device",
        "is_skipped",
        "source",
    )
)

with_details = (
    prepared.alias("s")
    .join(df_listeners.alias("l"), F.col("l.id") == F.col("s.listener_id"), "inner")
    .select(
        "s.id",
        "s.listener_id",
        "s.track_id",
        "s.played_at",
        "s.ms_played",
        "s.is_skipped",
        F.col("l.interests").alias("listener_interests"),
        F.col("l.plan").alias("listener_plan"),
        F.col("l.country").alias("listener_country"),
    )
)

filtered = (
    with_details.alias("s")
    .join(df_plays.alias("p"), (F.col("p.track_id") == F.col("s.track_id")) & (~F.col("p.is_skipped")), "left_anti")
)

tagged = (
    filtered
    .filter(F.col("track_id") >= 14)
    .select(
        "id",
        "listener_id",
        "track_id",
        "ms_played",
        "listener_interests",
        "listener_country",
        F.when(F.col("ms_played") >= 261579, "A").when(F.col("ms_played") >= 71476, "B").otherwise("C").alias("ms_played_tier"),
        F.initcap(F.trim(F.col("listener_country"))).alias("listener_country_clean"),
    )
)

result = (
    tagged
    .groupBy("listener_id")
    .pivot("listener_country", ["TH", "BR"])
    .agg(F.sum("ms_played"))
)
