from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_all_by_played_at_ties = Window.orderBy("played_at")
w_device_listener_country_by_p = Window.partitionBy("device", "listener_country").orderBy(F.col("played_at").asc_nulls_last())
w_all_by_played_at = Window.orderBy("played_at", "id")
w_device_listener_country_by_p_2 = Window.partitionBy("device", "listener_country").orderBy(F.col("played_at").asc_nulls_last(), "id")
w_track_id_by_played_at = Window.partitionBy("track_id").orderBy(F.col("played_at").desc(), "id")
w_track_id_by_played_at_ties = Window.partitionBy("track_id").orderBy(F.col("played_at").desc_nulls_first())
w_listener_id_listener_country = Window.partitionBy("listener_id", "listener_country").orderBy(F.col("played_at").desc(), "id")
w_played_at_dense_rank_2_bucke = Window.partitionBy("played_at_dense_rank_2_bucket", "device")
w_played_at_dense_rank_listene = Window.partitionBy("played_at_dense_rank", "listener_id").orderBy(F.col("played_at_percent_rank").desc(), "id").rowsBetween(-2, -1)
w_played_at_dense_rank_listene_2 = Window.partitionBy("played_at_dense_rank", "listener_id").orderBy(F.col("played_at_percent_rank").desc())

cleaned = (
    df_plays
    .filter((F.col("device") == "car") & (F.col("ms_played").between(72464, 257789)))
    .select(
        "id",
        "listener_id",
        "track_id",
        "played_at",
        "device",
        "source",
    )
)

enriched_listener = (
    cleaned.alias("s")
    .join(df_listeners.alias("l"), F.col("l.id") == F.col("s.listener_id"), "inner")
    .select("s.*", F.col("l.country").alias("listener_country"))
)

with_trends = (
    enriched_listener
    .select(
        "id",
        "listener_id",
        "track_id",
        "played_at",
        "device",
        "listener_country",
        F.round(F.percent_rank().over(w_all_by_played_at_ties), 3).alias("played_at_percent_rank"),
        F.round(F.cume_dist().over(w_device_listener_country_by_p), 3).alias("played_at_cume_dist"),
        F.dense_rank().over(w_all_by_played_at).alias("played_at_dense_rank"),
        F.ntile(5).over(w_device_listener_country_by_p_2).alias("played_at_quintile"),
        F.round((F.unix_timestamp("played_at") - F.unix_timestamp(F.lag("played_at").over(w_all_by_played_at))) / 60, 1).alias("minutes_since_prev_played_at"),
        F.round((F.unix_timestamp("played_at") - F.unix_timestamp(F.lag("played_at").over(w_track_id_by_played_at))) / 60, 1).alias("minutes_since_prev_played_at_2"),
        F.round(F.percent_rank().over(w_track_id_by_played_at), 3).alias("played_at_percent_rank_2"),
        F.dense_rank().over(w_track_id_by_played_at_ties).alias("played_at_dense_rank_2"),
        F.dense_rank().over(w_track_id_by_played_at).alias("played_at_dense_rank_3"),
        F.ntile(5).over(w_track_id_by_played_at).alias("played_at_quintile_2"),
        F.round((F.unix_timestamp("played_at") - F.unix_timestamp(F.lag("played_at").over(w_device_listener_country_by_p_2))) / 60, 1).alias("minutes_since_prev_played_at_3"),
        F.ntile(10).over(w_listener_id_listener_country).alias("played_at_decile"),
    )
)

scored = (
    with_trends
    .select(
        "*",
        (F.floor(F.col("played_at_dense_rank_2") / 2) * 2).alias("played_at_dense_rank_2_bucket"),
        F.substring(F.trim(F.col("device")), 1, 4).alias("device_prefix"),
    )
)

sequenced = (
    scored
    .select(
        "id",
        "track_id",
        "played_at",
        "device",
        "listener_country",
        "played_at_percent_rank",
        "played_at_cume_dist",
        "played_at_dense_rank",
        "played_at_quintile",
        "minutes_since_prev_played_at",
        "minutes_since_prev_played_at_2",
        "played_at_percent_rank_2",
        "played_at_dense_rank_2",
        "played_at_dense_rank_3",
        "played_at_quintile_2",
        "played_at_decile",
        "device_prefix",
        F.min("played_at_quintile").over(w_played_at_dense_rank_2_bucke).alias("played_at_dense_rank_2_bucket_device_min_played_"),
        F.max("played_at_decile").over(w_played_at_dense_rank_listene).alias("played_at_decile_max_2_prior"),
        F.round(F.avg("played_at_percent_rank").over(w_played_at_dense_rank_2_bucke), 2).alias("played_at_dense_rank_2_bucket_device_avg_played_"),
        F.round(F.col("played_at_dense_rank") - F.avg("played_at_dense_rank").over(w_played_at_dense_rank_2_bucke), 2).alias("played_at_dense_rank_vs_played_at_dense_rank_2_b"),
        F.dense_rank().over(w_played_at_dense_rank_listene_2).alias("played_at_percent_rank_dense_rank"),
        F.round(F.col("played_at_quintile_2") - F.avg("played_at_quintile_2").over(w_played_at_dense_rank_2_bucke), 2).alias("played_at_quintile_2_vs_played_at_dense_rank_2_b"),
    )
)

result = (
    sequenced
    .filter(F.col("played_at_quintile_2").between(3, 10))
    .select(
        "id",
        "played_at",
        "listener_country",
        "played_at_cume_dist",
        "played_at_dense_rank",
        "played_at_percent_rank_2",
        "played_at_dense_rank_2",
        "played_at_dense_rank_3",
        "played_at_quintile_2",
        "played_at_decile",
        "device_prefix",
        "played_at_decile_max_2_prior",
        "played_at_dense_rank_2_bucket_device_avg_played_",
        "played_at_dense_rank_vs_played_at_dense_rank_2_b",
        "played_at_percent_rank_dense_rank",
        "played_at_quintile_2_vs_played_at_dense_rank_2_b",
    )
    .orderBy(
        F.col("played_at_percent_rank_dense_rank").asc_nulls_last(),
        F.col("played_at_decile_max_2_prior").desc(),
    )
)
