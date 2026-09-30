from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_latest = Window.partitionBy("country", "genre").orderBy(F.col("is_verified_flag").desc(), F.col("id").desc())

recent = (
    df_artists
    .filter((F.col("country").isin("BR", "US", "TH")) & (F.col("artist_name").isNotNull()))
    .select(
        "id",
        "artist_name",
        "genre",
        "country",
        "debut_year",
        "is_verified",
        F.when(F.col("is_verified"), 1).otherwise(0).alias("is_verified_flag"),
    )
)

qualified = (
    recent.alias("s")
    .join(df_tracks.alias("t"), (F.col("t.artist_id") == F.col("s.id")) & (~F.col("t.is_explicit")), "left_semi")
    .filter(F.col("country").isin("JP", "BR"))
)

unique_rows = (
    qualified
    .withColumn("recency_rank", F.row_number().over(w_latest))
    .filter(F.col("recency_rank") == 1)
    .drop("recency_rank")
)

scored = (
    unique_rows
    .withColumn("genre_ref", F.concat(F.col("genre"), F.lit("#"), F.col("debut_year").cast("string")))
)

result = (
    scored
    .cube("is_verified", "genre", "is_verified_flag")
    .agg(
        F.grouping("is_verified").alias("is_total_is_verified"),
        F.round(F.avg("is_verified_flag"), 2).alias("avg_is_verified_flag"),
    )
)
