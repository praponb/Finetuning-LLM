from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_all_by_played_at = Window.orderBy(F.col("played_at").desc(), "id")
w_all_by_played_at_ties = Window.orderBy(F.col("played_at").desc())

base = (
    df_plays
    .filter((~F.col("is_skipped")) & (F.month(F.col("played_at")).isin(1, 2)))
    .select(
        "id",
        "listener_id",
        "track_id",
        "played_at",
        "ms_played",
        "device",
        "source",
        F.year(F.col("played_at")).alias("played_at_year"),
        F.length(F.trim(F.col("source"))).alias("source_len"),
    )
)

with_parent_track = (
    base.alias("s")
    .join(df_tracks.alias("t"), F.col("t.id") == F.col("s.track_id"), "left")
    .select("s.*", F.col("t.release_date").alias("track_release_date"))
)

filtered = (
    with_parent_track.alias("s")
    .join(df_plays.alias("p"), (F.col("p.track_id") == F.col("s.track_id")) & (F.col("p.ms_played") > 130888), "left_semi")
)

windowed = (
    filtered
    .select(
        "id",
        "listener_id",
        "track_id",
        "played_at",
        "ms_played",
        "device",
        "played_at_year",
        "source_len",
        F.lead("source_len", 2).over(w_all_by_played_at).alias("next2_source_len"),
        F.ntile(3).over(w_all_by_played_at).alias("played_at_tercile"),
        (F.col("source_len") - F.lag("source_len").over(w_all_by_played_at)).alias("source_len_change"),
        F.round(F.cume_dist().over(w_all_by_played_at_ties), 3).alias("played_at_cume_dist"),
    )
)

result = (
    windowed
    .rollup("played_at_tercile", "device", "listener_id")
    .agg(F.count("*").alias("row_count"))
    .select(
        "played_at_tercile",
        F.coalesce(F.col("device"), F.lit("ALL")).alias("device"),
        "listener_id",
        "row_count",
    )
)
