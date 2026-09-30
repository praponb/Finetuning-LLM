from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_is_explicit_by_duration_sec = Window.partitionBy("is_explicit").orderBy(F.col("duration_sec").asc_nulls_last(), "id")
w_is_explicit_artist_id = Window.partitionBy("is_explicit", "artist_id")
w_is_explicit_by_duration_sec_2 = Window.partitionBy("is_explicit").orderBy(F.col("duration_sec").asc_nulls_last())
w_is_explicit_by_duration_sec_3 = Window.partitionBy("is_explicit").orderBy(F.col("duration_sec").asc_nulls_last(), "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

filtered_tracks = (
    df_tracks
    .filter((F.col("is_explicit")) & (F.upper(F.trim(F.col("title"))).like("M%")))
    .select(
        "id",
        "artist_id",
        "title",
        "duration_sec",
        "is_explicit",
        "bpm",
        "moods",
    )
)

sequenced = (
    filtered_tracks
    .select(
        "*",
        (F.col("bpm") - F.lag("bpm").over(w_is_explicit_by_duration_sec)).alias("bpm_change"),
        F.sum("bpm").over(w_is_explicit_artist_id).alias("is_explicit_artist_id_sum_bpm"),
        F.rank().over(w_is_explicit_by_duration_sec_2).alias("duration_sec_rank"),
        F.sum("duration_sec").over(w_is_explicit_by_duration_sec_3).alias("running_duration_sec"),
    )
)

qualified = (
    sequenced.alias("s")
    .join(df_artists.alias("a"), (F.col("a.id") == F.col("s.artist_id")) & (F.upper(F.trim(F.col("a.artist_name"))).like("K%")), "left_anti")
)

joined_artist = (
    qualified.alias("s")
    .join(df_artists.alias("a"), F.col("a.id") == F.col("s.artist_id"), "inner")
    .filter(~F.col("a.is_verified"))
    .select(
        "s.id",
        "s.artist_id",
        "s.title",
        "s.duration_sec",
        "s.is_explicit",
        "s.bpm",
        "s.moods",
        "s.bpm_change",
        "s.is_explicit_artist_id_sum_bpm",
        "s.duration_sec_rank",
        "s.running_duration_sec",
        F.col("a.is_verified").alias("artist_is_verified"),
    )
)

result = (
    joined_artist
    .select("id", "bpm", "bpm_change")
    .unpivot(
        ids=["id"],
        values=["bpm", "bpm_change"],
        variableColumnName="field",
        valueColumnName="value",
    )
    .filter(F.col("value").isNotNull())
)
