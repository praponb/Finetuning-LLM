from pyspark.sql import functions as F

scoped = (
    df_tracks
    .filter((F.length(F.trim(F.col("title"))) >= 6) & (F.col("bpm").between(75, 170)))
    .select(
        "id",
        "artist_id",
        "title",
        "duration_sec",
        "release_date",
        F.add_months(F.col("release_date"), 12).alias("release_date_plus_12m"),
        F.size(F.col("moods")).alias("moods_count"),
        F.dayofweek(F.col("release_date")).alias("release_date_dayofweek"),
    )
)

tagged = (
    scoped
    .filter(F.col("release_date_dayofweek") > 3)
    .withColumn("least_duration_sec_release_date_dayofweek", F.least(F.col("duration_sec"), F.col("release_date_dayofweek")))
)

with_activity = (
    tagged
    .crossJoin(tagged.agg(F.max("duration_sec").alias("overall_max_duration_sec")))
    .filter(F.col("duration_sec") < F.col("overall_max_duration_sec"))
    .drop("overall_max_duration_sec")
)

screened = (
    with_activity.alias("s")
    .join(df_plays.alias("p"), (F.col("p.track_id") == F.col("s.id")) & (F.col("p.source").isin("album", "playlist")), "left_anti")
)

result = (
    screened
    .select("id", "duration_sec", "least_duration_sec_release_date_dayofweek")
    .unpivot(
        ids=["id"],
        values=["duration_sec", "least_duration_sec_release_date_dayofweek"],
        variableColumnName="measure",
        valueColumnName="amount",
    )
    .filter(F.col("amount").isNotNull())
)
