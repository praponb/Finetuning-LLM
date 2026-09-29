from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_track_id = Window.partitionBy("track_id")
w_played_at_to_date_track_id_b = Window.partitionBy(F.to_date("played_at"), "track_id").orderBy("ms_played")

base = (
    df_plays
    .filter(F.col("track_id").isNotNull())
    .select(
        "id",
        "listener_id",
        "track_id",
        "played_at",
        "ms_played",
        "device",
        "source",
        F.date_format(F.col("played_at"), "MMM yyyy").alias("played_at_label"),
        F.substring(F.trim(F.col("device")), 1, 2).alias("device_prefix"),
        F.coalesce(F.col("ms_played"), F.lit(0)).alias("ms_played_filled"),
    )
)

with_trends = (
    base
    .select(
        "*",
        F.round(F.col("ms_played_filled") / F.nullif(F.sum("ms_played_filled").over(w_track_id), F.lit(0)), 4).alias("ms_played_filled_share_of_track_id"),
        F.sum("ms_played").over(w_track_id).alias("track_id_sum_ms_played"),
        F.rank().over(w_played_at_to_date_track_id_b).alias("ms_played_rank"),
    )
)

leaders = with_trends.filter((F.col("ms_played_rank") == 1) & (F.col("ms_played") <= 324058))

totals = (
    leaders
    .groupBy(F.date_trunc("month", "played_at").alias("played_at_month"))
    .agg(
        F.min("played_at").alias("first_played_at"),
        F.round(F.avg("ms_played_filled_share_of_track_id"), 2).alias("avg_ms_played_filled_share_of_track_id"),
        F.max("played_at").alias("last_played_at"),
    )
)

result = (
    totals
    .select(
        "played_at_month",
        "first_played_at",
        "last_played_at",
        F.date_trunc("quarter", F.col("last_played_at")).alias("last_played_at_quarter"),
    )
    .orderBy("played_at_month", F.col("last_played_at").desc_nulls_first())
)
