from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_play_id_play_track_id_by_pla = Window.partitionBy("play_id", "play_track_id").orderBy("play_played_at_2", "id", "interest", "play_id", "play_id_2").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_play_id_play_track_id_by_pla_2 = Window.partitionBy("play_id", "play_track_id").orderBy("play_played_at_2", "id", "interest", "play_id", "play_id_2").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_play_id_play_track_id_by_pla_3 = Window.partitionBy("play_id", "play_track_id").orderBy("play_played_at_2", "id", "interest", "play_id", "play_id_2")
w_play_id_play_track_id_by_pla_4 = Window.partitionBy("play_id", "play_track_id").orderBy("play_played_at_2")

valid = (
    df_listeners
    .filter(F.col("birth_year") >= 1966)
    .select(
        "id",
        "username",
        "country",
        "signup_date",
        "birth_year",
        "interests",
        F.lower(F.regexp_replace(F.trim(F.col("plan")), " ", "_")).alias("plan_slug"),
        F.coalesce(F.col("country"), F.lit("UNKNOWN")).alias("country_or_unknown"),
    )
)

enriched = (
    valid
    .select(
        "*",
        F.date_add(F.col("signup_date"), 30).alias("signup_date_plus_30d"),
        F.abs(F.col("birth_year") - 1970).alias("birth_year_distance"),
    )
)

labeled = (
    enriched
    .filter(F.col("country_or_unknown").isin("KR", "BR"))
    .select(
        "*",
        F.array_join(F.array_sort(F.col("interests")), ",").alias("interests_text"),
        F.datediff(F.lit("2025-06-30").cast("date"), F.col("signup_date_plus_30d")).alias("days_since_signup_date_plus_30d"),
        F.concat(F.col("plan_slug"), F.lit("#"), F.col("birth_year").cast("string")).alias("plan_slug_ref"),
    )
)

exploded = (
    labeled
    .select(
        "id",
        "username",
        "country",
        "signup_date",
        "birth_year",
        "plan_slug",
        "country_or_unknown",
        "signup_date_plus_30d",
        "birth_year_distance",
        "interests_text",
        "days_since_signup_date_plus_30d",
        "plan_slug_ref",
        F.explode("interests").alias("interest"),
    )
)

with_children = (
    exploded.alias("s")
    .join(df_plays.alias("p"), F.col("p.listener_id") == F.col("s.id"), "left")
    .select(
        "s.id",
        "s.country",
        "s.country_or_unknown",
        "s.interests_text",
        "s.days_since_signup_date_plus_30d",
        "s.plan_slug_ref",
        "s.interest",
        F.col("p.id").alias("play_id"),
        F.col("p.played_at").alias("play_played_at"),
        F.col("p.source").alias("play_source"),
        F.col("p.device").alias("play_device"),
    )
)

exploded_detail = (
    with_children.alias("s")
    .join(df_plays.alias("p"), F.col("p.listener_id") == F.col("s.id"), "inner")
    .select(
        "s.id",
        "s.country",
        "s.country_or_unknown",
        "s.interests_text",
        "s.plan_slug_ref",
        "s.interest",
        "s.play_id",
        "s.play_played_at",
        "s.play_source",
        "s.play_device",
        F.col("p.id").alias("play_id_2"),
        F.col("p.is_skipped").alias("play_is_skipped"),
        F.col("p.played_at").alias("play_played_at_2"),
        F.col("p.track_id").alias("play_track_id"),
    )
)

ranked = (
    exploded_detail
    .select(
        "*",
        F.last("play_played_at", ignorenulls=True).over(w_play_id_play_track_id_by_pla).alias("last_known_play_played_at"),
        F.first("play_played_at_2").over(w_play_id_play_track_id_by_pla_2).alias("first_play_played_at_2"),
        F.round(F.cume_dist().over(w_play_id_play_track_id_by_pla_3), 3).alias("play_played_at_2_cume_dist"),
        F.round(F.percent_rank().over(w_play_id_play_track_id_by_pla_4), 3).alias("play_played_at_2_percent_rank"),
    )
)

result = (
    ranked
    .select(
        "id",
        "interest",
        "play_played_at_2_cume_dist",
        "play_played_at_2_percent_rank",
    )
    .unpivot(
        ids=["id", "interest"],
        values=["play_played_at_2_cume_dist", "play_played_at_2_percent_rank"],
        variableColumnName="measure",
        valueColumnName="metric_value",
    )
    .filter(F.col("metric_value").isNotNull())
    .orderBy("id", "interest", "measure")
)
