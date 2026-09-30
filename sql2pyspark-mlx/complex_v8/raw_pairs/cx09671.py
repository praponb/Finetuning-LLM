from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_mood_artist_id = Window.partitionBy("mood", "artist_id")
w_artist_id_mood_by_release_da = Window.partitionBy("artist_id", "mood").orderBy(F.col("release_date").desc(), "id", "mood")

prepared = (
    df_tracks
    .filter(~F.col("is_explicit"))
    .select(
        "id",
        "artist_id",
        "duration_sec",
        "release_date",
        "moods",
    )
)

flattened = (
    prepared
    .select(
        "id",
        "artist_id",
        "duration_sec",
        "release_date",
        F.explode("moods").alias("mood"),
    )
)

sequenced = (
    flattened
    .select(
        "id",
        "artist_id",
        "mood",
        F.round(F.col("duration_sec") / F.nullif(F.sum("duration_sec").over(w_mood_artist_id), F.lit(0)), 4).alias("duration_sec_share_of_mood_artist_id"),
        F.datediff(F.col("release_date"), F.lag("release_date").over(w_artist_id_mood_by_release_da)).alias("days_since_prev_release_date"),
        F.round(F.col("duration_sec") - F.avg("duration_sec").over(w_mood_artist_id), 2).alias("duration_sec_vs_mood_artist_id_avg"),
    )
)

result = (
    sequenced
    .filter(F.col("mood").isin("chill", "focus"))
    .select(
        "id",
        "artist_id",
        "mood",
        "days_since_prev_release_date",
        "duration_sec_vs_mood_artist_id_avg",
        F.concat(F.col("mood"), F.lit("#"), F.col("artist_id").cast("string")).alias("mood_ref"),
    )
    .orderBy(F.col("mood").asc_nulls_last())
)
