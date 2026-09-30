from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_played_at_month_by_played_at = Window.partitionBy(F.date_trunc("month", "played_at")).orderBy(F.col("played_at").desc_nulls_first(), "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_all_by_played_at = Window.orderBy("played_at", "id")
w_played_at_month_by_played_at_2 = Window.partitionBy(F.date_trunc("month", "played_at")).orderBy(F.col("played_at").desc_nulls_first(), "id")
w_source_track_id_by_played_at = Window.partitionBy("source", "track_id").orderBy(F.col("played_at").asc_nulls_last(), "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_source_track_id_by_played_at_2 = Window.partitionBy("source", "track_id").orderBy(F.col("played_at").asc_nulls_last(), "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_source_track_id_by_played_at_3 = Window.partitionBy("source", "track_id").orderBy(F.col("played_at").asc_nulls_last(), "id")
w_source_track_id_by_played_at_4 = Window.partitionBy("source", "track_id").orderBy("played_at", "id")
w_source_track_id_by_played_at_5 = Window.partitionBy("source", "track_id").orderBy("played_at")

recent_plays = (
    df_plays
    .filter((F.col("track_id") >= 6) & (F.col("listener_id").isNotNull()))
    .select(
        "id",
        "listener_id",
        "track_id",
        "played_at",
        "device",
        "source",
    )
)

ranked = (
    recent_plays
    .select(
        "*",
        F.nth_value("played_at", 2).over(w_played_at_month_by_played_at).alias("second_played_at"),
        F.rank().over(w_all_by_played_at).alias("played_at_rank"),
        F.row_number().over(w_played_at_month_by_played_at_2).alias("played_at_seq"),
        F.first("played_at", ignorenulls=True).over(w_source_track_id_by_played_at).alias("first_known_played_at"),
        F.nth_value("played_at", 2).over(w_source_track_id_by_played_at_2).alias("second_played_at_2"),
        F.round(F.percent_rank().over(w_source_track_id_by_played_at_3), 3).alias("played_at_percent_rank"),
        F.ntile(10).over(w_source_track_id_by_played_at_4).alias("played_at_decile"),
        F.round(F.percent_rank().over(w_source_track_id_by_played_at_5), 3).alias("played_at_percent_rank_2"),
        F.row_number().over(w_source_track_id_by_played_at_3).alias("rn"),
        F.round(F.percent_rank().over(w_all_by_played_at), 3).alias("played_at_percent_rank_3"),
        F.ntile(4).over(w_played_at_month_by_played_at_2).alias("played_at_quartile"),
    )
)

scored = (
    ranked
    .filter(F.hour(F.col("played_at")).between(7, 11))
    .select(
        "*",
        F.date_format(F.col("first_known_played_at"), "yyyy-MM-dd").alias("first_known_played_at_label"),
        F.greatest(F.col("played_at_rank"), F.col("played_at_decile")).alias("greatest_played_at_rank_played_at_decile"),
        F.coalesce(F.col("device"), F.lit("UNKNOWN")).alias("device_or_unknown"),
    )
)

totals = (
    scored
    .filter(F.col("played_at_percent_rank_3") <= 0.8)
    .groupBy(
        F.date_trunc("month", "second_played_at").alias("second_played_at_month"),
    )
    .agg(
        F.count("*").alias("n_rows"),
        F.max("second_played_at_2").alias("last_second_played_at_2"),
        F.max("first_known_played_at").alias("last_first_known_played_at"),
        F.round(F.sum("played_at_percent_rank_3") / F.nullif(F.sum("played_at_quartile"), F.lit(0)), 3).alias("played_at_percent_rank_3_to_played_at_quartile_r"),
        F.sum(F.when(F.col("device").isin("tv", "car"), 1).otherwise(0)).alias("n_device_in_tv_car"),
    )
)

result = (
    totals
    .select(
        "second_played_at_month",
        "n_rows",
        "last_second_played_at_2",
        "played_at_percent_rank_3_to_played_at_quartile_r",
        F.datediff(F.lit("2026-01-01").cast("date"), F.col("last_second_played_at_2")).alias("days_since_last_second_played_at_2"),
    )
    .orderBy(
        F.col("second_played_at_month").desc(),
        "days_since_last_second_played_at_2",
    )
)
