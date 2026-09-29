from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_all_by_distinct_track_ids = Window.orderBy(F.col("distinct_track_ids").desc(), "device", "first_played_at_month", "first_played_at_3_month")
w_last_first_played_at_3_month = Window.partitionBy(F.month("last_first_played_at_3"), "device").orderBy("distinct_track_ids", "device", "first_played_at_month", "first_played_at_3_month").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_first_played_at_3_month_mont = Window.partitionBy(F.month("first_played_at_3_month"), F.year("first_played_at_month")).orderBy(F.col("first_first_played_at_3").desc(), "device", "first_played_at_month", "first_played_at_3_month").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_last_first_played_at_3_month_2 = Window.partitionBy(F.month("last_first_played_at_3"), "device").orderBy("distinct_track_ids")

filtered_plays = (
    df_plays
    .filter((F.col("listener_id") >= 12) & (F.col("ms_played") < 218129))
    .select(
        "id",
        "listener_id",
        "track_id",
        "ms_played",
        "device",
        "source",
        F.date_format(F.col("played_at"), "yyyy-MM").alias("played_at_label"),
        (F.floor(F.col("ms_played") / 50000) * 50000).alias("ms_played_bucket"),
        (F.floor(F.col("ms_played") / 50000) * 50000).alias("ms_played_bucket_2"),
    )
)

play_stats = (
    df_plays
    .groupBy("listener_id")
    .agg(
        F.min("played_at").alias("first_played_at"),
        F.min("played_at").alias("first_played_at_2"),
        F.min("played_at").alias("first_played_at_3"),
    )
)

summarized_rows = (
    filtered_plays.alias("s")
    .join(play_stats.alias("p"), F.col("p.listener_id") == F.col("s.listener_id"), "left")
    .select(
        "s.*",
        "p.first_played_at",
        "p.first_played_at_2",
        "p.first_played_at_3",
    )
)

with_details = (
    summarized_rows.alias("s")
    .join(df_tracks.alias("t"), F.col("t.id") == F.col("s.track_id"), "inner")
    .select(
        "s.*",
        F.col("t.release_date").alias("track_release_date"),
        F.col("t.moods").alias("track_moods"),
        F.col("t.artist_id").alias("track_artist_id"),
    )
)

totals = (
    with_details
    .groupBy(
        "device",
        F.date_trunc("month", "first_played_at").alias("first_played_at_month"),
        F.date_trunc("month", "first_played_at_3").alias("first_played_at_3_month"),
    )
    .agg(
        F.countDistinct("track_id").alias("distinct_track_ids"),
        F.max("first_played_at_3").alias("last_first_played_at_3"),
        F.min("first_played_at_3").alias("first_first_played_at_3"),
    )
)

windowed = (
    totals
    .select(
        "*",
        F.lead("first_played_at_3_month").over(w_all_by_distinct_track_ids).alias("next_first_played_at_3_month"),
        F.first("first_played_at_3_month", ignorenulls=True).over(w_last_first_played_at_3_month).alias("first_known_first_played_at_3_month"),
        F.nth_value("first_played_at_month", 2).over(w_first_played_at_3_month_mont).alias("second_first_played_at_month"),
        F.dense_rank().over(w_last_first_played_at_3_month_2).alias("distinct_track_ids_dense_rank"),
    )
)

derived = (
    windowed
    .select(
        "device",
        "first_played_at_month",
        "first_played_at_3_month",
        "first_first_played_at_3",
        "second_first_played_at_month",
        "distinct_track_ids_dense_rank",
        F.when(F.col("device").isin("speaker", "desktop"), "group_a").when(F.col("device") == "phone", "group_b").otherwise("other").alias("device_group"),
    )
)

top_rows = derived.filter(F.col("distinct_track_ids_dense_rank") == 1)

result = (
    top_rows
    .select(
        "device",
        "first_played_at_month",
        "first_played_at_3_month",
        "second_first_played_at_month",
        "device_group",
        F.date_trunc("month", F.col("first_first_played_at_3")).alias("first_first_played_at_3_month"),
    )
    .orderBy(F.col("first_played_at_month").asc())
)
