from pyspark.sql import functions as F

prepared_listeners = (
    df_listeners
    .filter(F.year(F.col("signup_date")) == 2025)
    .select(
        "id",
        "username",
        "plan",
        "country",
        "signup_date",
        "birth_year",
    )
)

detail_rows_plays = (
    prepared_listeners.alias("s")
    .join(df_plays.alias("p"), F.col("p.listener_id") == F.col("s.id"), "inner")
    .select(
        "s.id",
        "s.username",
        "s.plan",
        "s.country",
        "s.signup_date",
        "s.birth_year",
        F.col("p.id").alias("play_id"),
        F.col("p.played_at").alias("play_played_at"),
        F.col("p.device").alias("play_device"),
    )
)

result = (
    detail_rows_plays
    .cube("play_device", "play_id", "country")
    .agg(
        F.round(F.stddev("birth_year"), 2).alias("stddev_birth_year"),
        F.count("*").alias("row_count"),
    )
    .orderBy(
        F.col("play_device").asc_nulls_last(),
        F.col("play_id").asc_nulls_last(),
        F.col("country").asc_nulls_last(),
    )
)
