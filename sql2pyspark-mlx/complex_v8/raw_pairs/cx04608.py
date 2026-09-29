from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_latest = Window.partitionBy("artist_id").orderBy(F.col("bpm").desc(), F.col("id").desc())

prepared = (
    df_tracks
    .filter(F.col("duration_sec") <= 402)
    .select(
        "id",
        "artist_id",
        "title",
        "duration_sec",
        "is_explicit",
        "bpm",
        "moods",
        F.lower(F.regexp_replace(F.trim(F.col("title")), " ", "_")).alias("title_slug"),
    )
)

without_matches = (
    prepared.alias("s")
    .join(df_artists.alias("a"), (F.col("a.id") == F.col("s.artist_id")) & (F.col("a.country").isNotNull()) & (F.col("a.debut_year") < 2016), "left_semi")
)

deduped = (
    without_matches
    .withColumn("row_num", F.row_number().over(w_latest))
    .filter(F.col("row_num") == 1)
    .drop("row_num")
)

result = (
    deduped
    .select("id", "bpm", "duration_sec")
    .unpivot(
        ids=["id"],
        values=["bpm", "duration_sec"],
        variableColumnName="metric",
        valueColumnName="reading",
    )
    .filter(F.col("reading").isNotNull())
)
