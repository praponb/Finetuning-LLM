from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_is_explicit_by_release_date = Window.partitionBy("is_explicit").orderBy(F.col("release_date").desc(), "id")
w_release_date_month = Window.partitionBy(F.date_trunc("month", "release_date"))
w_all_by_release_date = Window.orderBy(F.col("release_date").asc_nulls_last(), "id")

scoped = (
    df_tracks
    .filter(F.col("artist_id").isNotNull())
    .select(
        "id",
        "artist_id",
        "title",
        "release_date",
        "is_explicit",
        "bpm",
        "moods",
    )
)

scored = (
    scoped
    .filter(F.col("artist_id") <= 11)
    .select(
        "*",
        F.upper(F.trim(F.col("title"))).alias("title_clean"),
        F.substring(F.trim(F.col("title")), 1, 2).alias("title_prefix"),
    )
)

windowed = (
    scored
    .select(
        "id",
        "artist_id",
        "title",
        "release_date",
        "is_explicit",
        "bpm",
        "moods",
        "title_prefix",
        F.datediff(F.col("release_date"), F.lag("release_date").over(w_is_explicit_by_release_date)).alias("days_since_prev_release_date"),
        F.size(F.collect_set("artist_id").over(w_release_date_month)).alias("distinct_artist_id_in_release_date_month"),
        F.lag("release_date").over(w_all_by_release_date).alias("prev_release_date"),
        F.row_number().over(w_all_by_release_date).alias("seq_no"),
        F.lag("release_date").over(w_is_explicit_by_release_date).alias("prev_release_date_2"),
    )
)

per_label = (
    windowed
    .select(
        "id",
        "artist_id",
        "title",
        "release_date",
        "is_explicit",
        "bpm",
        "title_prefix",
        "days_since_prev_release_date",
        "distinct_artist_id_in_release_date_month",
        "prev_release_date",
        "seq_no",
        "prev_release_date_2",
        F.posexplode_outer("moods").alias("mood_pos", "mood"),
    )
)

tagged = (
    per_label
    .filter(F.col("prev_release_date_2") < F.lit("2024-12-14").cast("date"))
    .select(
        "*",
        F.when(F.col("mood").isin("sad", "chill"), "group_a").when(F.col("mood") == "workout", "group_b").otherwise("other").alias("mood_group"),
        F.concat_ws(": ", F.col("title"), F.col("title_prefix")).alias("title_title_prefix_label"),
    )
)

joined = (
    tagged.alias("s")
    .join(df_artists.alias("a"), F.col("a.id") == F.col("s.artist_id"), "left")
    .select(
        "s.id",
        "s.artist_id",
        "s.title",
        "s.bpm",
        "s.title_prefix",
        "s.days_since_prev_release_date",
        "s.distinct_artist_id_in_release_date_month",
        "s.prev_release_date",
        "s.mood_pos",
        "s.mood",
        "s.title_title_prefix_label",
        F.col("a.is_verified").alias("artist_is_verified"),
        F.col("a.genre").alias("artist_genre"),
    )
)

play_stats = (
    df_plays
    .groupBy("track_id")
    .agg(F.max("ms_played").alias("max_play_ms_played"))
)

summarized_rows = (
    joined.alias("s")
    .join(play_stats.alias("p"), F.col("p.track_id") == F.col("s.id"), "left")
    .select("s.*", "p.max_play_ms_played")
)

result = (
    summarized_rows
    .select(
        "id",
        "mood_pos",
        "distinct_artist_id_in_release_date_month",
        "days_since_prev_release_date",
    )
    .unpivot(
        ids=["id", "mood_pos"],
        values=["distinct_artist_id_in_release_date_month", "days_since_prev_release_date"],
        variableColumnName="field",
        valueColumnName="value",
    )
    .filter(F.col("value").isNotNull())
)
