from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_latest = Window.partitionBy("device", "source").orderBy(F.col("days_since_played_at").desc(), F.col("id").desc())
w_listener_plan_n_listener_pla = Window.partitionBy("listener_plan", "n_listener_plan_not_in_premium").orderBy(F.col("n_listener_plan_not_in_premium").desc(), "listener_plan", "listener_country", "played_at_ym").rowsBetween(-1, Window.currentRow)
w_listener_plan = Window.partitionBy("listener_plan")

prepared_plays = (
    df_plays
    .filter((F.col("source") == "search") & (~F.col("is_skipped")))
    .select(
        "id",
        "listener_id",
        "track_id",
        "played_at",
        "ms_played",
        "device",
        "is_skipped",
        "source",
        F.datediff(F.lit("2025-06-30").cast("date"), F.col("played_at")).alias("days_since_played_at"),
        F.abs(F.col("ms_played") - 223262).alias("ms_played_distance"),
        F.coalesce(F.col("ms_played"), F.lit(0)).alias("ms_played_filled"),
    )
)

deduped = (
    prepared_plays
    .withColumn("recency_rank", F.row_number().over(w_latest))
    .filter(F.col("recency_rank") == 1)
    .drop("recency_rank")
)

with_details = (
    deduped.alias("s")
    .join(df_listeners.alias("l"), (F.col("l.id") == F.col("s.listener_id")) & (F.col("l.country") == "BR"), "inner")
    .filter(F.col("l.country").isin("TH", "BR", "KR"))
    .select(
        "s.id",
        "s.listener_id",
        "s.track_id",
        "s.played_at",
        "s.ms_played",
        "s.is_skipped",
        "s.source",
        "s.days_since_played_at",
        "s.ms_played_distance",
        "s.ms_played_filled",
        F.col("l.country").alias("listener_country"),
        F.col("l.plan").alias("listener_plan"),
    )
)

summary = (
    with_details
    .filter(F.col("played_at") >= F.lit("2025-01-19 13:00:00").cast("timestamp"))
    .groupBy(
        "listener_plan",
        "listener_country",
        F.date_format("played_at", "yyyy-MM").alias("played_at_ym"),
    )
    .agg(
        F.sum("ms_played_distance").alias("total_ms_played_distance"),
        F.round(F.avg("ms_played_filled"), 2).alias("avg_ms_played_filled"),
        F.count(F.when(~F.col("listener_plan").isin("premium", "family"), 1)).alias("n_listener_plan_not_in_premium"),
    )
)

ranked = (
    summary
    .select(
        "*",
        F.round(F.avg("avg_ms_played_filled").over(w_listener_plan_n_listener_pla), 2).alias("avg_ms_played_filled_avg_2"),
        F.round(F.avg("total_ms_played_distance").over(w_listener_plan), 2).alias("listener_plan_avg_total_ms_played_distance"),
        F.size(F.collect_set("listener_country").over(w_listener_plan)).alias("distinct_listener_country_in_listener_plan"),
    )
)

result = (
    ranked
    .select(
        "listener_plan",
        "listener_country",
        "total_ms_played_distance",
        "avg_ms_played_filled",
        "listener_plan_avg_total_ms_played_distance",
        "distinct_listener_country_in_listener_plan",
        (F.col("total_ms_played_distance") - F.col("avg_ms_played_filled")).alias("total_ms_played_distance_minus_avg_ms_played_fil"),
        (F.col("total_ms_played_distance") - F.col("avg_ms_played_filled")).alias("total_ms_played_distance_minus_avg_ms_played_fil_2"),
    )
)
