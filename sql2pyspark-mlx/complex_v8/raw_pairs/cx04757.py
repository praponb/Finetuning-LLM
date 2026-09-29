from pyspark.sql import functions as F

cleaned = (
    df_listeners
    .filter(F.col("birth_year") < 1994)
    .select(
        "id",
        "username",
        "plan",
        "signup_date",
        "birth_year",
        "interests",
        F.initcap(F.trim(F.col("username"))).alias("username_clean"),
        F.size(F.col("interests")).alias("interests_count"),
        F.concat_ws("|", F.col("username"), F.col("country")).alias("username_country_label"),
    )
)

tagged = (
    cleaned
    .filter(F.col("birth_year").between(1968, 1994))
    .select(
        "id",
        "signup_date",
        "birth_year",
        "interests",
        "username_clean",
        "interests_count",
        (F.col("birth_year") + F.col("interests_count")).alias("birth_year_plus_interests_count"),
        F.size(F.col("interests")).alias("interests_count_2"),
    )
)

play_stats = (
    df_plays
    .groupBy("listener_id")
    .agg(
        F.count("*").alias("play_count"),
        F.min("ms_played").alias("min_play_ms_played"),
    )
)

summarized_rows = (
    tagged.alias("s")
    .join(play_stats.alias("p"), F.col("p.listener_id") == F.col("s.id"), "left")
    .select(
        "s.*",
        F.coalesce(F.col("p.play_count"), F.lit(0)).alias("play_count"),
        "p.min_play_ms_played",
    )
)

with_children = (
    summarized_rows.alias("s")
    .join(df_plays.alias("p"), F.col("p.listener_id") == F.col("s.id"), "left")
    .select(
        "s.id",
        "s.signup_date",
        "s.username_clean",
        "s.interests_count",
        "s.birth_year_plus_interests_count",
        "s.interests_count_2",
        "s.play_count",
        "s.min_play_ms_played",
        F.col("p.id").alias("play_id"),
        F.col("p.device").alias("play_device"),
        F.col("p.played_at").alias("play_played_at"),
        F.col("p.is_skipped").alias("play_is_skipped"),
        F.col("p.source").alias("play_source"),
    )
)

result = (
    with_children
    .rollup("play_device", "play_id", "play_source")
    .agg(
        F.array_sort(F.collect_set("play_source")).alias("play_source_list"),
        F.count("*").alias("cnt"),
        F.min("interests_count").alias("min_interests_count"),
    )
    .orderBy(
        F.col("play_device").asc_nulls_last(),
        F.col("play_id").asc_nulls_last(),
        F.col("play_source").asc_nulls_last(),
    )
)
