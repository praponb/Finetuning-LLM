from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_play_count_2 = Window.partitionBy("play_count_2")

recent = (
    df_listeners
    .filter(F.col("birth_year") <= 1990)
    .select(
        "id",
        "plan",
        "signup_date",
        "birth_year",
        "interests",
    )
)

play_stats = (
    df_plays
    .groupBy("listener_id")
    .agg(
        F.count("*").alias("play_count"),
        F.round(F.avg("ms_played"), 2).alias("avg_play_ms_played"),
        F.count("*").alias("play_count_2"),
    )
)

with_counts = (
    recent.alias("s")
    .join(play_stats.alias("p"), F.col("p.listener_id") == F.col("s.id"), "left")
    .select(
        "s.*",
        F.coalesce(F.col("p.play_count"), F.lit(0)).alias("play_count"),
        "p.avg_play_ms_played",
        F.coalesce(F.col("p.play_count_2"), F.lit(0)).alias("play_count_2"),
    )
)

labeled = (
    with_counts
    .select(
        "*",
        F.when(F.col("plan").isin("premium", "family"), "group_a").when(F.col("plan") == "free", "group_b").otherwise("other").alias("plan_group"),
        F.date_format(F.col("signup_date"), "yyyy-MM-dd").alias("signup_date_label"),
        (F.floor(F.col("birth_year") / 10) * 10).alias("birth_year_bucket"),
    )
)

metrics = (
    labeled
    .select(
        "id",
        "plan",
        "signup_date",
        "birth_year",
        "interests",
        "play_count_2",
        "signup_date_label",
        "birth_year_bucket",
        F.max("play_count").over(w_play_count_2).alias("play_count_2_max_play_count"),
        F.min("birth_year_bucket").over(w_play_count_2).alias("play_count_2_min_birth_year_bucket"),
    )
)

screened = (
    metrics
    .crossJoin(metrics.agg(F.min("play_count_2").alias("overall_min_play_count_2")))
    .filter(F.col("play_count_2") > F.col("overall_min_play_count_2"))
    .drop("overall_min_play_count_2")
)

annotated = (
    screened
    .select(
        "id",
        "signup_date",
        "birth_year",
        "interests",
        "signup_date_label",
        "play_count_2_min_birth_year_bucket",
        F.coalesce(F.col("play_count_2"), F.lit(0)).alias("play_count_2_filled"),
    )
)

with_activity = (
    annotated.alias("s")
    .join(df_plays.alias("p"), (F.col("p.listener_id") == F.col("s.id")) & (~F.col("p.is_skipped")), "left_anti")
    .filter(F.col("signup_date") >= F.lit("2024-07-13").cast("date"))
)

result = (
    with_activity
    .select(
        "id",
        "signup_date",
        "interests",
        "signup_date_label",
        "play_count_2_filled",
        (F.floor(F.col("birth_year") / 10) * 10).alias("birth_year_bucket"),
    )
    .orderBy(F.col("id").asc())
    .limit(5)
)
