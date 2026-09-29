from pyspark.sql import functions as F

track_side = (
    df_tracks
    .filter(~F.col("is_explicit"))
    .select("id")
)

track_side_2 = (
    df_tracks
    .filter((F.upper(F.trim(F.col("title"))).like("G%")) & (F.col("duration_sec") > 183))
    .select(F.col("duration_sec").alias("id"))
)

result = (
    track_side
    .exceptAll(track_side_2)
    .orderBy(F.col("id").desc())
)
