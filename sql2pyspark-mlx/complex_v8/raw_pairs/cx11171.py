from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_play_is_skipped_count_by_bir = Window.partitionBy("play_is_skipped_count").orderBy(F.col("birth_year").desc(), "id").rowsBetween(-2, Window.currentRow)
w_all_by_total_play_ms_played = Window.orderBy("total_play_ms_played", "id")
w_play_is_skipped_count_by_bir_2 = Window.partitionBy("play_is_skipped_count").orderBy(F.col("birth_year").desc(), "id")
w_all_by_total_play_ms_played_2 = Window.orderBy("total_play_ms_played", "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_country_play_is_skipped_coun = Window.partitionBy("country", "play_is_skipped_count").orderBy(F.col("first_birth_year").asc_nulls_last(), "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_country_play_is_skipped_coun_2 = Window.partitionBy("country", "play_is_skipped_count").orderBy(F.col("first_birth_year").asc_nulls_last(), "id")
w_country_plan_by_total_play_m = Window.partitionBy("country", "plan").orderBy("total_play_ms_played_change", "id")

base = (
    df_listeners
    .filter((F.array_contains(F.col("interests"), "indie")) & (F.col("plan") == "student"))
    .select(
        "id",
        "username",
        "plan",
        "country",
        "signup_date",
        "birth_year",
        "interests",
    )
)

play_stats = (
    df_plays
    .groupBy("listener_id")
    .agg(
        F.min("ms_played").alias("min_play_ms_played"),
        F.count(F.when(~F.col("is_skipped"), 1)).alias("play_is_skipped_count"),
        F.sum("ms_played").alias("total_play_ms_played"),
    )
)

with_stats = (
    base.alias("s")
    .join(play_stats.alias("p"), F.col("p.listener_id") == F.col("s.id"), "left")
    .select(
        "s.*",
        "p.min_play_ms_played",
        F.coalesce(F.col("p.play_is_skipped_count"), F.lit(0)).alias("play_is_skipped_count"),
        "p.total_play_ms_played",
    )
)

sequenced = (
    with_stats
    .select(
        "id",
        "username",
        "plan",
        "country",
        "interests",
        "play_is_skipped_count",
        "total_play_ms_played",
        F.round(F.avg("play_is_skipped_count").over(w_play_is_skipped_count_by_bir), 2).alias("play_is_skipped_count_avg_3"),
        F.round(F.cume_dist().over(w_all_by_total_play_ms_played), 3).alias("total_play_ms_played_cume_dist"),
        (F.col("total_play_ms_played") - F.lag("total_play_ms_played").over(w_play_is_skipped_count_by_bir_2)).alias("total_play_ms_played_change"),
        F.first("birth_year").over(w_all_by_total_play_ms_played_2).alias("first_birth_year"),
    )
)

metrics = (
    sequenced
    .select(
        "id",
        "plan",
        "interests",
        "play_is_skipped_count",
        "play_is_skipped_count_avg_3",
        F.count(F.lit(1)).over(w_country_play_is_skipped_coun).alias("running_count"),
        F.row_number().over(w_country_play_is_skipped_coun_2).alias("first_birth_year_seq"),
        F.rank().over(w_country_plan_by_total_play_m).alias("total_play_ms_played_change_rank"),
    )
)

play_stats_2 = (
    df_plays
    .groupBy("listener_id")
    .agg(
        F.min("played_at").alias("first_played_at"),
        F.count(F.when(~F.col("device").isin("phone", "tv"), 1)).alias("play_device_count"),
    )
)

with_stats_2 = (
    metrics.alias("s")
    .join(play_stats_2.alias("p"), F.col("p.listener_id") == F.col("s.id"), "left")
    .select(
        "s.*",
        "p.first_played_at",
        F.coalesce(F.col("p.play_device_count"), F.lit(0)).alias("play_device_count"),
    )
)

play_stats_3 = (
    df_plays
    .groupBy("listener_id")
    .agg(F.max("played_at").alias("last_played_at"))
)

profiled = (
    with_stats_2.alias("s")
    .join(play_stats_3.alias("p"), F.col("p.listener_id") == F.col("s.id"), "left")
    .select("s.*", "p.last_played_at")
)

summary = (
    profiled
    .groupBy(
        F.date_format("last_played_at", "yyyy-MM").alias("last_played_at_ym"),
        F.to_date("first_played_at").alias("first_played_at_day"),
    )
    .agg(
        F.round(F.sum("play_is_skipped_count") / F.nullif(F.sum("first_birth_year_seq"), F.lit(0)), 3).alias("play_is_skipped_count_to_first_birth_year_seq_ra"),
        F.max("play_is_skipped_count_avg_3").alias("max_play_is_skipped_count_avg_3"),
        F.sum("total_play_ms_played_change_rank").alias("total_total_play_ms_played_change_rank"),
    )
)

result = (
    summary
    .filter(F.col("total_total_play_ms_played_change_rank") >= 13)
    .select(
        "last_played_at_ym",
        "first_played_at_day",
        "play_is_skipped_count_to_first_birth_year_seq_ra",
        "max_play_is_skipped_count_avg_3",
        "total_total_play_ms_played_change_rank",
        F.lower(F.regexp_replace(F.trim(F.col("last_played_at_ym")), " ", "_")).alias("last_played_at_ym_slug"),
        F.lower(F.regexp_replace(F.trim(F.col("last_played_at_ym")), " ", "_")).alias("last_played_at_ym_slug_2"),
    )
)
