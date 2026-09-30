from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_dedup = Window.partitionBy("source_or_unknown").orderBy(F.col("played_at_month").desc(), F.col("id").desc())
w_device_group_by_ms_played = Window.partitionBy("device_group").orderBy("ms_played", "id")
w_is_skipped = Window.partitionBy("is_skipped")
w_track_id_by_played_at_month = Window.partitionBy("track_id").orderBy(F.col("played_at_month").desc(), "id")
w_device_group_by_ms_played_pr = Window.partitionBy("device_group").orderBy("ms_played", "id").rowsBetween(-6, -1)

filtered = (
    df_plays
    .filter(F.col("ms_played") < 241585)
    .select(
        "id",
        "listener_id",
        "track_id",
        "played_at",
        "ms_played",
        "device",
        "is_skipped",
        "source",
        F.when(F.col("device").isin("car", "desktop"), "group_a").when(F.col("device") == "tv", "group_b").otherwise("other").alias("device_group"),
        F.month(F.col("played_at")).alias("played_at_month"),
    )
)

filtered_2 = (
    filtered.alias("s")
    .join(df_plays.alias("p"), (F.col("p.listener_id") == F.col("s.listener_id")) & (F.col("p.device") != "car") & (F.hour(F.col("p.played_at")).between(8, 13)), "left_anti")
    .filter(F.col("device").isin("car", "tv", "phone"))
)

annotated = (
    filtered_2
    .select(
        "*",
        (F.col("played_at") + F.expr("INTERVAL 7 DAYS")).alias("played_at_plus_7d"),
        F.when(F.col("is_skipped"), "yes").otherwise("no").alias("is_skipped_label"),
        F.coalesce(F.col("source"), F.lit("UNKNOWN")).alias("source_or_unknown"),
    )
)

latest_per_key = (
    annotated
    .withColumn("recency_rank", F.row_number().over(w_dedup))
    .filter(F.col("recency_rank") == 1)
    .drop("recency_rank")
)

windowed = (
    latest_per_key
    .select(
        "id",
        "listener_id",
        "track_id",
        "played_at",
        "ms_played",
        "device",
        "is_skipped",
        "device_group",
        "played_at_month",
        "is_skipped_label",
        "source_or_unknown",
        F.lead("played_at_plus_7d", 3).over(w_device_group_by_ms_played).alias("next3_played_at_plus_7d"),
        F.count(F.lit(1)).over(w_is_skipped).alias("rows_in_is_skipped"),
        F.rank().over(w_track_id_by_played_at_month).alias("played_at_month_rank"),
        F.round(F.avg("ms_played").over(w_device_group_by_ms_played_pr), 2).alias("ms_played_avg_6_prior"),
    )
)

with_parent_track = (
    windowed.alias("s")
    .join(df_tracks.alias("t"), F.col("t.id") == F.col("s.track_id"), "inner")
    .join(df_listeners.alias("l"), F.col("l.id") == F.col("s.listener_id"), "left")
    .select(
        "s.id",
        "s.listener_id",
        "s.track_id",
        "s.device",
        "s.played_at_month",
        "s.is_skipped_label",
        "s.next3_played_at_plus_7d",
        F.col("t.moods").alias("track_moods"),
        F.col("t.is_explicit").alias("track_is_explicit"),
        F.col("t.bpm").alias("track_bpm"),
        F.col("l.plan").alias("listener_plan"),
        F.col("l.interests").alias("listener_interests"),
    )
)

result = (
    with_parent_track
    .groupBy("track_is_explicit")
    .pivot("device", ["speaker", "phone", "tv", "desktop"])
    .agg(F.avg("track_bpm").alias("avg_v"), F.sum("track_bpm").alias("sum_v"))
    .orderBy("track_is_explicit")
)
