from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_all_by_birth_year = Window.orderBy(F.col("birth_year").desc(), "id", "play_id")
w_play_is_skipped = Window.partitionBy("play_is_skipped")

recent = (
    df_listeners
    .filter(F.col("birth_year") < 1981)
    .select(
        "id",
        "plan",
        "country",
        "birth_year",
        "interests",
        F.date_format(F.col("signup_date"), "yyyy-MM-dd").alias("signup_date_label"),
        F.dayofweek(F.col("signup_date")).alias("signup_date_dayofweek"),
        F.abs(F.col("birth_year") - 1970).alias("birth_year_distance"),
    )
)

exploded_detail_plays = (
    recent.alias("s")
    .join(df_plays.alias("p"), F.col("p.listener_id") == F.col("s.id"), "inner")
    .select(
        "s.id",
        "s.plan",
        "s.country",
        "s.birth_year",
        "s.signup_date_dayofweek",
        "s.birth_year_distance",
        F.col("p.id").alias("play_id"),
        F.col("p.ms_played").alias("play_ms_played"),
        F.col("p.source").alias("play_source"),
        F.col("p.track_id").alias("play_track_id"),
        F.col("p.is_skipped").alias("play_is_skipped"),
    )
)

with_details_play = (
    exploded_detail_plays.alias("s")
    .join(df_plays.alias("p"), F.col("p.id") == F.col("s.play_id"), "inner")
    .select(
        "s.id",
        "s.plan",
        "s.birth_year",
        "s.signup_date_dayofweek",
        "s.birth_year_distance",
        "s.play_id",
        "s.play_source",
        "s.play_track_id",
        "s.play_is_skipped",
        F.col("p.played_at").alias("play_played_at"),
        F.col("p.source").alias("play_source_2"),
    )
)

with_windows = (
    with_details_play
    .select(
        "id",
        "signup_date_dayofweek",
        "play_id",
        "play_source",
        "play_track_id",
        "play_played_at",
        F.lag("birth_year_distance", 3, 0).over(w_all_by_birth_year).alias("prev3_birth_year_distance"),
        F.round(F.col("signup_date_dayofweek") / F.nullif(F.sum("signup_date_dayofweek").over(w_play_is_skipped), F.lit(0)), 4).alias("signup_date_dayofweek_share_of_play_is_skipped"),
    )
)

result = (
    with_windows
    .groupBy("play_played_at", "play_id")
    .pivot("play_source", ["search", "album", "playlist", "radio"])
    .agg(F.sum("prev3_birth_year_distance").alias("amount"), F.max("prev3_birth_year_distance").alias("max_v"))
    .orderBy("play_played_at", "play_id")
)
