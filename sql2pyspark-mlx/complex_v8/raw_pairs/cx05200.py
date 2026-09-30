from pyspark.sql import functions as F

valid_plays = (
    df_plays
    .filter((F.col("source").isin("search", "album")) & (F.col("is_skipped")))
    .select(
        "id",
        "listener_id",
        "track_id",
        "played_at",
        "ms_played",
        "device",
        F.coalesce(F.col("ms_played"), F.lit(0)).alias("ms_played_filled"),
    )
)

filtered = (
    valid_plays.alias("s")
    .join(df_plays.alias("p"), (F.col("p.track_id") == F.col("s.track_id")) & (F.col("p.device") != "tv") & (F.col("p.listener_id") >= 18), "left_anti")
)

without_matches = (
    filtered.alias("s")
    .join(df_plays.alias("p"), F.col("p.listener_id") == F.col("s.listener_id"), "left_semi")
)

result = (
    without_matches
    .select("id", "ms_played", "ms_played_filled")
    .unpivot(
        ids=["id"],
        values=["ms_played", "ms_played_filled"],
        variableColumnName="measure",
        valueColumnName="amount",
    )
)
