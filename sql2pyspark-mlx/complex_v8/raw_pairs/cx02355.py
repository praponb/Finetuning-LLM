from pyspark.sql import functions as F

cleaned = (
    df_listeners
    .filter(F.col("plan") == "student")
    .select(
        "id",
        "username",
        "country",
        "signup_date",
        "interests",
        F.when(F.col("plan").isin("family", "student"), "group_a").when(F.col("plan") == "free", "group_b").otherwise("other").alias("plan_group"),
    )
)

play_stats = (
    df_plays
    .groupBy("listener_id")
    .agg(
        F.min("played_at").alias("first_played_at"),
        F.count("*").alias("play_count"),
    )
)

with_counts = (
    cleaned.alias("s")
    .join(play_stats.alias("p"), F.col("p.listener_id") == F.col("s.id"), "left")
    .select(
        "s.*",
        "p.first_played_at",
        F.coalesce(F.col("p.play_count"), F.lit(0)).alias("play_count"),
    )
)

derived = (
    with_counts
    .filter(F.col("play_count") >= 2)
    .select(
        "*",
        F.coalesce(F.col("play_count"), F.lit(0)).alias("play_count_filled"),
        F.coalesce(F.col("play_count"), F.lit(0)).alias("play_count_filled_2"),
        F.concat_ws(" / ", F.col("username"), F.col("country")).alias("username_country_label"),
    )
)

result = (
    derived
    .groupingSets(
        [["play_count_filled", "plan_group", "play_count"], ["play_count_filled"]],
        "play_count_filled", "plan_group", "play_count",
    )
    .agg(
        F.grouping("play_count_filled").alias("is_total_play_count_filled"),
        F.countDistinct("plan_group").alias("distinct_plan_groups"),
        F.min("signup_date").alias("first_signup_date"),
    )
    .select(
        "play_count_filled",
        F.coalesce(F.col("plan_group"), F.lit("ALL")).alias("plan_group"),
        "play_count",
        "is_total_play_count_filled",
        "distinct_plan_groups",
        "first_signup_date",
    )
    .orderBy(
        F.col("play_count_filled").asc_nulls_last(),
        F.col("plan_group").asc_nulls_last(),
        F.col("play_count").asc_nulls_last(),
    )
)
