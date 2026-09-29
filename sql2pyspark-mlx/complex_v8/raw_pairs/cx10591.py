from pyspark.sql import functions as F

recent = (
    df_listeners
    .filter(F.col("birth_year").between(1966, 1981))
    .select(
        "id",
        "username",
        "plan",
        "signup_date",
        "birth_year",
        "interests",
    )
)

filtered = (
    recent.alias("s")
    .join(df_plays.alias("p"), (F.col("p.listener_id") == F.col("s.id")) & (F.col("p.ms_played") >= 157900) & (~F.col("p.device").isin("phone", "desktop")), "left_anti")
)

play_stats = (
    df_plays
    .groupBy("listener_id")
    .agg(
        F.count("*").alias("play_count"),
        F.max("played_at").alias("last_played_at"),
        F.count("*").alias("play_count_2"),
    )
)

profiled = (
    filtered.alias("s")
    .join(play_stats.alias("p"), F.col("p.listener_id") == F.col("s.id"), "left")
    .select(
        "s.*",
        F.coalesce(F.col("p.play_count"), F.lit(0)).alias("play_count"),
        "p.last_played_at",
        F.coalesce(F.col("p.play_count_2"), F.lit(0)).alias("play_count_2"),
    )
)

annotated = (
    profiled
    .select(
        "*",
        F.when(F.col("plan").isin("student", "free"), "group_a").when(F.col("plan") == "premium", "group_b").otherwise("other").alias("plan_group"),
    )
)

result = (
    annotated
    .select(
        "id",
        "plan",
        "signup_date",
        "birth_year",
        "interests",
        "play_count_2",
        "plan_group",
        (F.col("play_count_2") + F.col("play_count")).alias("play_count_2_plus_play_count"),
    )
    .orderBy("plan_group", F.col("play_count_2_plus_play_count").desc_nulls_first())
)
