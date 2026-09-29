from pyspark.sql import functions as F

prepared = (
    df_listeners
    .select(
        "id",
        "username",
        "plan",
        "birth_year",
        "interests",
    )
)

scored = (
    prepared
    .filter(F.array_contains(F.col("interests"), "jazz"))
    .select(
        "*",
        (F.floor(F.col("birth_year") / 10) * 10).alias("birth_year_bucket"),
        F.concat_ws(" / ", F.col("username"), F.col("plan")).alias("username_plan_label"),
        F.coalesce(F.col("birth_year"), F.lit(0)).alias("birth_year_filled"),
    )
)

derived = (
    scored
    .select(
        "*",
        F.round(F.col("birth_year_filled") / F.nullif(F.col("birth_year"), F.lit(0)), 3).alias("birth_year_filled_per_birth_year"),
        F.concat_ws(" / ", F.col("username"), F.col("plan")).alias("username_plan_label_2"),
        F.coalesce(F.col("birth_year"), F.lit(0)).alias("birth_year_filled_2"),
    )
)

play_stats = (
    df_plays
    .groupBy("listener_id")
    .agg(
        F.count("*").alias("play_count"),
        F.min("played_at").alias("first_played_at"),
        F.count(F.when(F.col("device").isNotNull(), 1)).alias("play_device_count"),
    )
)

with_stats = (
    derived.alias("s")
    .join(play_stats.alias("p"), F.col("p.listener_id") == F.col("s.id"), "left")
    .select(
        "s.*",
        F.coalesce(F.col("p.play_count"), F.lit(0)).alias("play_count"),
        "p.first_played_at",
        F.coalesce(F.col("p.play_device_count"), F.lit(0)).alias("play_device_count"),
    )
)

result = (
    with_stats
    .select(
        "id",
        "username",
        "plan",
        "birth_year_bucket",
        "username_plan_label",
        "birth_year_filled_per_birth_year",
        "username_plan_label_2",
        "birth_year_filled_2",
        "play_count",
        "first_played_at",
        "play_device_count",
        (F.col("birth_year") - F.col("play_count")).alias("birth_year_minus_play_count"),
        (F.floor(F.col("birth_year_filled") / 10) * 10).alias("birth_year_filled_bucket"),
    )
    .orderBy(F.col("username_plan_label_2").asc(), "id")
    .limit(5)
)
