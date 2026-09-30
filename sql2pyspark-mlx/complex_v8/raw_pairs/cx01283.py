from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_track_id_is_skipped = Window.partitionBy("track_id", "is_skipped")
w_is_skipped_listener_id_by_ms = Window.partitionBy("is_skipped", "listener_id").orderBy(F.col("ms_played").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_track_id_by_played_at = Window.partitionBy("track_id").orderBy("played_at", "id")

prepared = (
    df_plays
    .filter(F.col("device") == "phone")
    .select(
        "id",
        "listener_id",
        "track_id",
        "played_at",
        "ms_played",
        "is_skipped",
    )
)

qualified = (
    prepared.alias("s")
    .join(df_plays.alias("p"), (F.col("p.track_id") == F.col("s.track_id")) & (F.col("p.is_skipped")), "left_anti")
    .filter(~F.col("is_skipped"))
)

sequenced = (
    qualified
    .select(
        "id",
        "listener_id",
        "played_at",
        "is_skipped",
        F.round(F.col("ms_played") / F.nullif(F.sum("ms_played").over(w_track_id_is_skipped), F.lit(0)), 4).alias("ms_played_share_of_track_id_is_skipped"),
        F.nth_value("played_at", 2).over(w_is_skipped_listener_id_by_ms).alias("second_played_at"),
        F.min("ms_played").over(w_track_id_is_skipped).alias("track_id_is_skipped_min_ms_played"),
        F.ntile(10).over(w_track_id_by_played_at).alias("played_at_decile"),
    )
)

result = (
    sequenced
    .select("id", "played_at_decile", "track_id_is_skipped_min_ms_played")
    .unpivot(
        ids=["id"],
        values=["played_at_decile", "track_id_is_skipped_min_ms_played"],
        variableColumnName="metric_name",
        valueColumnName="reading",
    )
    .filter(F.col("reading").isNotNull())
)
