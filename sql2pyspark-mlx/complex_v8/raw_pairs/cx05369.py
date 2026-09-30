from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_dedup = Window.partitionBy("debut_year_tier", "debut_year_tier_2").orderBy("is_verified_flag", "id")

valid_artists = (
    df_artists
    .filter(F.lower(F.col("artist_name")).like("%ht%"))
    .select(
        "id",
        "artist_name",
        "genre",
        "debut_year",
        "is_verified",
        F.when(F.col("debut_year") >= 2006, "A").when(F.col("debut_year") >= 1996, "B").otherwise("C").alias("debut_year_tier"),
        F.when(F.col("is_verified"), 1).otherwise(0).alias("is_verified_flag"),
        F.when(F.col("debut_year") >= 2006, "HIGH").when(F.col("debut_year") >= 1997, "MEDIUM").otherwise("LOW").alias("debut_year_tier_2"),
    )
)

latest_per_key = (
    valid_artists
    .withColumn("rn", F.row_number().over(w_dedup))
    .filter(F.col("rn") == 1)
    .drop("rn")
)

with_activity = (
    latest_per_key
    .crossJoin(latest_per_key.agg(F.avg("is_verified_flag").alias("overall_avg_is_verified_flag")))
    .filter(F.col("is_verified_flag") > F.col("overall_avg_is_verified_flag"))
    .drop("overall_avg_is_verified_flag")
)

labeled = (
    with_activity
    .select(
        "id",
        "artist_name",
        "debut_year",
        "is_verified",
        "is_verified_flag",
        "debut_year_tier_2",
        F.concat_ws(" / ", F.col("debut_year_tier"), F.col("artist_name")).alias("debut_year_tier_artist_name_label"),
    )
)

result = (
    labeled
    .filter(F.col("is_verified_flag") >= 0)
    .select(
        "id",
        "artist_name",
        "debut_year",
        "is_verified",
        "is_verified_flag",
        "debut_year_tier_artist_name_label",
        F.coalesce(F.col("debut_year"), F.lit(0)).alias("debut_year_filled"),
    )
    .orderBy("debut_year_tier_artist_name_label")
)
