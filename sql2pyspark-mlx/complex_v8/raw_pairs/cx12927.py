from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_listener_id_by_ms_played = Window.partitionBy("listener_id").orderBy("ms_played", "id")
w_source = Window.partitionBy("source")
w_listener_id_by_played_at_run = Window.partitionBy("listener_id").orderBy("played_at", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_listener_id_by_played_at = Window.partitionBy("listener_id").orderBy("played_at", "id")
w_played_at_to_date = Window.partitionBy(F.to_date("played_at"))
w_all_by_ms_played_seq = Window.orderBy("ms_played_seq", "id")
w_all_by_ms_played_seq_ties = Window.orderBy("ms_played_seq")

filtered_plays = (
    df_plays
    .filter((F.col("ms_played") >= 56858) & (~F.col("is_skipped")))
    .select(
        "id",
        "listener_id",
        "track_id",
        "played_at",
        "ms_played",
        "source",
    )
)

ranked = (
    filtered_plays
    .select(
        "id",
        "listener_id",
        "track_id",
        "played_at",
        "source",
        F.lead("played_at").over(w_listener_id_by_ms_played).alias("next_played_at"),
        F.count(F.lit(1)).over(w_source).alias("rows_in_source"),
        F.min("ms_played").over(w_listener_id_by_played_at_run).alias("min_ms_played_so_far"),
        F.lag("played_at").over(w_listener_id_by_played_at).alias("prev_played_at"),
        F.row_number().over(w_listener_id_by_ms_played).alias("ms_played_seq"),
    )
)

with_windows = (
    ranked
    .select(
        "*",
        F.round(F.col("min_ms_played_so_far") / F.nullif(F.sum("min_ms_played_so_far").over(w_played_at_to_date), F.lit(0)), 4).alias("min_ms_played_so_far_share_of_played_at_to_date"),
        F.ntile(3).over(w_all_by_ms_played_seq).alias("ms_played_seq_tercile"),
        F.rank().over(w_all_by_ms_played_seq_ties).alias("ms_played_seq_rank"),
        F.dense_rank().over(w_all_by_ms_played_seq_ties).alias("ms_played_seq_dense_rank"),
        F.sum("min_ms_played_so_far").over(w_played_at_to_date).alias("played_at_to_date_sum_min_ms_played_so_far"),
    )
)

combined_details = (
    with_windows.alias("s")
    .join(df_tracks.alias("t"), F.col("t.id") == F.col("s.track_id"), "inner")
    .join(df_artists.alias("a"), F.col("a.id") == F.col("t.artist_id"), "inner")
    .select(
        "s.id",
        "s.listener_id",
        "s.track_id",
        "s.played_at",
        "s.source",
        "s.next_played_at",
        "s.min_ms_played_so_far",
        "s.prev_played_at",
        "s.ms_played_seq_rank",
        "s.ms_played_seq_dense_rank",
        "s.played_at_to_date_sum_min_ms_played_so_far",
        F.col("t.is_explicit").alias("track_is_explicit"),
        F.col("a.country").alias("artist_country"),
        F.col("a.debut_year").alias("artist_debut_year"),
    )
)

result = (
    combined_details
    .select(
        "id",
        "track_id",
        "played_at",
        "source",
        "next_played_at",
        "prev_played_at",
        "ms_played_seq_dense_rank",
        "played_at_to_date_sum_min_ms_played_so_far",
        "track_is_explicit",
        "artist_country",
        (F.col("played_at") + F.expr("INTERVAL 7 DAYS")).alias("played_at_plus_7d"),
        F.when(F.col("artist_country").isin("KR", "JP"), "group_a").when(F.col("artist_country") == "TH", "group_b").otherwise("other").alias("artist_country_group"),
    )
)
