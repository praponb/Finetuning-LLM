from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_track_id_listener_country_by = Window.partitionBy("track_id", "listener_country").orderBy("played_at", "id")
w_track_id_listener_country_by_2 = Window.partitionBy("track_id", "listener_country").orderBy("played_at")
w_played_at_year_listener_id_b = Window.partitionBy(F.year("played_at"), "listener_id").orderBy("played_at")
w_played_at_year_listener_id_b_2 = Window.partitionBy(F.year("played_at"), "listener_id").orderBy("played_at", "id")
w_played_at_dense_rank_listene = Window.partitionBy("played_at_dense_rank", "listener_id")
w_played_at_year_by_played_at = Window.partitionBy(F.year("played_at")).orderBy("played_at_dense_rank_2", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

base_plays = (
    df_plays
    .filter(F.col("played_at") >= F.lit("2025-01-19 13:00:00").cast("timestamp"))
    .select(
        "id",
        "listener_id",
        "track_id",
        "played_at",
        "device",
        "is_skipped",
        "source",
        F.concat_ws("|", F.col("device"), F.col("source")).alias("device_source_label"),
        F.date_format(F.col("played_at"), "yyyy-MM-dd").alias("played_at_label"),
    )
)

combined_details_listener = (
    base_plays.alias("s")
    .join(df_listeners.alias("l"), F.col("l.id") == F.col("s.listener_id"), "inner")
    .select(
        "s.id",
        "s.listener_id",
        "s.track_id",
        "s.played_at",
        "s.source",
        "s.played_at_label",
        F.col("l.country").alias("listener_country"),
    )
)

sequenced = (
    combined_details_listener
    .select(
        "id",
        "listener_id",
        "played_at",
        "source",
        "played_at_label",
        "listener_country",
        F.rank().over(w_track_id_listener_country_by).alias("played_at_rank"),
        F.dense_rank().over(w_track_id_listener_country_by_2).alias("played_at_dense_rank"),
        F.round(F.cume_dist().over(w_played_at_year_listener_id_b), 3).alias("played_at_cume_dist"),
        F.row_number().over(w_played_at_year_listener_id_b_2).alias("seq_no"),
        F.dense_rank().over(w_played_at_year_listener_id_b).alias("played_at_dense_rank_2"),
    )
)

sequenced_2 = (
    sequenced
    .select(
        "id",
        "listener_id",
        "played_at",
        "source",
        "played_at_label",
        "listener_country",
        "played_at_rank",
        "seq_no",
        F.sum("played_at_dense_rank").over(w_played_at_dense_rank_listene).alias("played_at_dense_rank_listener_id_sum_played_at_d"),
        F.size(F.collect_set("listener_country").over(w_played_at_dense_rank_listene)).alias("distinct_listener_country_in_played_at_dense_ran"),
        F.max("played_at_dense_rank").over(w_played_at_year_by_played_at).alias("max_played_at_dense_rank_so_far"),
        F.size(F.collect_set("source").over(w_played_at_dense_rank_listene)).alias("distinct_source_in_played_at_dense_rank_listener"),
    )
)

annotated = (
    sequenced_2
    .select(
        "id",
        "listener_id",
        "played_at",
        "source",
        "played_at_label",
        "listener_country",
        "played_at_rank",
        "seq_no",
        "played_at_dense_rank_listener_id_sum_played_at_d",
        "max_played_at_dense_rank_so_far",
        "distinct_source_in_played_at_dense_rank_listener",
        F.substring(F.trim(F.col("source")), 1, 3).alias("source_prefix"),
        (F.col("played_at_rank") - F.col("seq_no")).alias("played_at_rank_minus_seq_no"),
    )
)

result = (
    annotated
    .select(
        "id",
        "listener_id",
        "played_at",
        "source",
        "listener_country",
        "played_at_dense_rank_listener_id_sum_played_at_d",
        "max_played_at_dense_rank_so_far",
        "distinct_source_in_played_at_dense_rank_listener",
        "source_prefix",
        "played_at_rank_minus_seq_no",
        (F.col("seq_no") + F.col("played_at_rank")).alias("seq_no_plus_played_at_rank"),
    )
    .orderBy("played_at_rank_minus_seq_no", F.col("played_at").asc())
)
