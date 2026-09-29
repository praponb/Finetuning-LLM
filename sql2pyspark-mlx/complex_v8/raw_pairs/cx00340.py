from pyspark.sql import functions as F

filtered_listeners = (
    df_listeners
    .filter(F.array_contains(F.col("interests"), "jazz"))
    .select(
        "id",
        "plan",
        "country",
        "birth_year",
        F.date_trunc("quarter", F.col("signup_date")).alias("signup_date_quarter"),
    )
)

play_stats = (
    df_plays
    .groupBy("listener_id")
    .agg(
        F.count("*").alias("play_count"),
        F.max("played_at").alias("last_played_at"),
    )
)

summarized_rows = (
    filtered_listeners.alias("s")
    .join(play_stats.alias("p"), F.col("p.listener_id") == F.col("s.id"), "left")
    .select(
        "s.*",
        F.coalesce(F.col("p.play_count"), F.lit(0)).alias("play_count"),
        "p.last_played_at",
    )
)

with_activity = (
    summarized_rows.alias("s")
    .join(df_plays.alias("p"), (F.col("p.listener_id") == F.col("s.id")) & (F.month(F.col("p.played_at")).isin(1, 3)), "left_anti")
)

result = (
    with_activity
    .cube("play_count", "country")
    .agg(
        F.grouping_id().alias("gid"),
        F.countDistinct("country").alias("distinct_countrys"),
    )
    .select(
        "play_count",
        F.coalesce(F.col("country"), F.lit("ALL")).alias("country"),
        "gid",
        "distinct_countrys",
    )
    .orderBy(F.col("play_count").asc_nulls_last(), F.col("country").asc_nulls_last())
)
