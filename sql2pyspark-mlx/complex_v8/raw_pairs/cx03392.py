from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_play_device_country_by_play = Window.partitionBy("play_device", "country").orderBy("play_played_at", "id", "play_id")
w_play_played_at_month_play_de = Window.partitionBy(F.month("play_played_at"), "play_device")

recent_listeners = (
    df_listeners
    .filter((F.col("username").isNotNull()) & (F.col("plan") == "student"))
    .select(
        "id",
        "plan",
        "country",
        "birth_year",
        "interests",
    )
)

line_level = (
    recent_listeners.alias("s")
    .join(df_plays.alias("p"), (F.col("p.listener_id") == F.col("s.id")) & (F.col("p.device") != "phone"), "inner")
    .select(
        "s.id",
        "s.country",
        "s.birth_year",
        "s.interests",
        F.col("p.id").alias("play_id"),
        F.col("p.device").alias("play_device"),
        F.col("p.source").alias("play_source"),
        F.col("p.played_at").alias("play_played_at"),
    )
)

sequenced = (
    line_level
    .select(
        "id",
        "country",
        "interests",
        "play_id",
        "play_device",
        "play_source",
        "play_played_at",
        (F.col("birth_year") - F.lag("birth_year").over(w_play_device_country_by_play)).alias("birth_year_change"),
        F.round(F.col("birth_year") / F.nullif(F.sum("birth_year").over(w_play_played_at_month_play_de), F.lit(0)), 4).alias("birth_year_share_of_play_played_at_month_play_de"),
    )
)

result = (
    sequenced
    .groupingSets(
        [["play_id", "country"], ["play_id"], ["country"]],
        "play_id", "country",
    )
    .agg(
        F.grouping_id().alias("gid"),
        F.round(F.sum("birth_year_share_of_play_played_at_month_play_de"), 2).alias("total_birth_year_share_of_play_played_at_month_p"),
        F.countDistinct("play_id").alias("distinct_play_ids"),
    )
)
