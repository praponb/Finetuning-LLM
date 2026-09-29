from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_all_by_birth_year_ties = Window.orderBy(F.col("birth_year").desc())
w_all_by_birth_year = Window.orderBy(F.col("birth_year").desc(), "id")
w_country = Window.partitionBy("country")
w_play_source = Window.partitionBy("play_source")
w_play_track_id_by_birth_year = Window.partitionBy("play_track_id").orderBy(F.col("birth_year_dense_rank").desc_nulls_first(), "id", "play_id")
w_all_by_birth_year_quartile = Window.orderBy(F.col("birth_year_quartile").asc_nulls_last(), "id", "play_id")
w_all_by_birth_year_dense_rank = Window.orderBy(F.col("birth_year_dense_rank").desc(), "id", "play_id")

filtered = (
    df_listeners
    .select(
        "id",
        "username",
        "country",
        "birth_year",
    )
)

ranked = (
    filtered
    .select(
        "id",
        "username",
        "country",
        "birth_year",
        F.dense_rank().over(w_all_by_birth_year_ties).alias("birth_year_dense_rank"),
        F.row_number().over(w_all_by_birth_year).alias("seq_no"),
        F.ntile(4).over(w_all_by_birth_year).alias("birth_year_quartile"),
        F.round(F.col("birth_year") / F.nullif(F.sum("birth_year").over(w_country), F.lit(0)), 4).alias("birth_year_share_of_country"),
    )
)

exploded_detail = (
    ranked.alias("s")
    .join(df_plays.alias("p"), F.col("p.listener_id") == F.col("s.id"), "inner")
    .select(
        "s.id",
        "s.username",
        "s.country",
        "s.birth_year_dense_rank",
        "s.birth_year_quartile",
        "s.birth_year_share_of_country",
        F.col("p.id").alias("play_id"),
        F.col("p.ms_played").alias("play_ms_played"),
        F.col("p.played_at").alias("play_played_at"),
        F.col("p.source").alias("play_source"),
        F.col("p.track_id").alias("play_track_id"),
    )
)

with_windows = (
    exploded_detail
    .select(
        "id",
        "username",
        "country",
        "birth_year_share_of_country",
        "play_id",
        "play_played_at",
        "play_source",
        "play_track_id",
        F.round(F.col("play_ms_played") / F.nullif(F.sum("play_ms_played").over(w_play_source), F.lit(0)), 4).alias("play_ms_played_share_of_play_source"),
        (F.col("birth_year_quartile") - F.lag("birth_year_quartile").over(w_play_track_id_by_birth_year)).alias("birth_year_quartile_change"),
        F.lead("birth_year_quartile", 3).over(w_all_by_birth_year_quartile).alias("next3_birth_year_quartile"),
        F.lead("birth_year_quartile", 2).over(w_all_by_birth_year_dense_rank).alias("next2_birth_year_quartile"),
    )
)

play_track_id_stats = (
    with_windows
    .groupBy("play_track_id")
    .agg(F.avg("birth_year_quartile_change").alias("avg_birth_year_quartile_change"))
)

outliers = (
    with_windows.alias("s")
    .join(play_track_id_stats.alias("g"), F.col("g.play_track_id") == F.col("s.play_track_id"), "inner")
    .filter(F.col("s.birth_year_quartile_change") > F.col("g.avg_birth_year_quartile_change"))
    .select(
        "s.*",
        "g.avg_birth_year_quartile_change",
        F.round(F.col("s.birth_year_quartile_change") - F.col("g.avg_birth_year_quartile_change"), 2).alias("birth_year_quartile_change_minus_avg"),
    )
)

result = (
    outliers
    .select(
        "id",
        "play_id",
        "play_ms_played_share_of_play_source",
        "birth_year_quartile_change_minus_avg",
        "avg_birth_year_quartile_change",
        "birth_year_share_of_country",
    )
    .unpivot(
        ids=["id", "play_id"],
        values=["play_ms_played_share_of_play_source", "birth_year_quartile_change_minus_avg", "avg_birth_year_quartile_change", "birth_year_share_of_country"],
        variableColumnName="measure",
        valueColumnName="metric_value",
    )
    .filter(F.col("metric_value").isNotNull())
    .orderBy("id", "play_id", "measure")
)
