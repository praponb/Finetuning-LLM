from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_bpm_tier_by_release_date = Window.partitionBy("bpm_tier").orderBy("release_date", "id")
w_release_date_month_is_explic = Window.partitionBy(F.month("release_date"), "is_explicit")
w_bpm_tier_by_release_date_tie = Window.partitionBy("bpm_tier").orderBy("release_date")

prepared = (
    df_tracks
    .filter((F.col("duration_sec") <= 353) | (~F.col("is_explicit")))
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

qualified = (
    prepared.alias("s")
    .join(df_artists.alias("a"), (F.col("a.id") == F.col("s.artist_id")) & (~F.col("a.is_verified")) & (F.col("a.country") == "BR"), "left_semi")
)

derived = (
    qualified
    .select(
        "*",
        F.month(F.col("release_date")).alias("release_date_month"),
        F.when(F.col("bpm") >= 143, "A").when(F.col("bpm") >= 104, "B").otherwise("C").alias("bpm_tier"),
    )
)

sequenced = (
    derived
    .select(
        "*",
        F.lead("release_date", 3).over(w_bpm_tier_by_release_date).alias("next3_release_date"),
        F.round(F.col("release_date_month") - F.avg("release_date_month").over(w_release_date_month_is_explic), 2).alias("release_date_month_vs_release_date_month_is_expl"),
        F.round(F.percent_rank().over(w_bpm_tier_by_release_date_tie), 3).alias("release_date_percent_rank"),
    )
)

result = (
    sequenced
    .cube("release_date_month", "bpm_tier")
    .agg(F.count("*").alias("n_rows"))
    .select(
        "release_date_month",
        F.coalesce(F.col("bpm_tier"), F.lit("ALL")).alias("bpm_tier"),
        "n_rows",
    )
)
