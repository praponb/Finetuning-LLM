from pyspark.sql import functions as F

cleaned_plays = (
    df_plays
    .filter(F.col("track_id") >= 15)
    .select(
        "id",
        "listener_id",
        "track_id",
        "played_at",
        "device",
        "source",
    )
)

scored = (
    cleaned_plays
    .filter(F.col("source") == "radio")
    .select(
        "id",
        "listener_id",
        "track_id",
        "device",
        "source",
        F.date_trunc("month", F.col("played_at")).alias("played_at_month"),
        (F.col("played_at") + F.expr("INTERVAL 1 DAYS")).alias("played_at_plus_1d"),
        F.year(F.col("played_at")).alias("played_at_year"),
    )
)

result = (
    scored
    .cube("track_id", "source")
    .agg(
        F.grouping_id().alias("gid"),
        F.round(F.avg("played_at_year"), 2).alias("avg_played_at_year"),
        F.min("played_at_year").alias("min_played_at_year"),
    )
    .orderBy(F.col("track_id").asc_nulls_last(), F.col("source").asc_nulls_last())
)
