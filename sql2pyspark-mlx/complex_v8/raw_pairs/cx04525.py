from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_play_device_count_signup_dat = Window.partitionBy("play_device_count", F.date_trunc("month", "signup_date_plus_365d")).orderBy("signup_date", "id").rowsBetween(-2, Window.currentRow)
w_play_device_count_by_last_pl = Window.partitionBy("play_device_count").orderBy("last_played_at", "id")

valid_listeners = (
    df_listeners
    .filter((F.col("birth_year") >= 1967) & (F.size(F.col("interests")) >= 1))
    .select(
        "id",
        "plan",
        "country",
        "signup_date",
        "interests",
    )
)

tagged = (
    valid_listeners
    .select(
        "id",
        "signup_date",
        "interests",
        F.date_add(F.col("signup_date"), 365).alias("signup_date_plus_365d"),
    )
)

play_stats = (
    df_plays
    .groupBy("listener_id")
    .agg(
        F.max("played_at").alias("last_played_at"),
        F.count(F.when(F.col("device").isin("desktop", "car", "tv"), 1)).alias("play_device_count"),
    )
)

summarized_rows = (
    tagged.alias("s")
    .join(play_stats.alias("p"), F.col("p.listener_id") == F.col("s.id"), "left")
    .select(
        "s.*",
        "p.last_played_at",
        F.coalesce(F.col("p.play_device_count"), F.lit(0)).alias("play_device_count"),
    )
)

sequenced = (
    summarized_rows
    .select(
        "*",
        F.round(F.avg("play_device_count").over(w_play_device_count_signup_dat), 2).alias("play_device_count_avg_3"),
        F.ntile(3).over(w_play_device_count_by_last_pl).alias("last_played_at_tercile"),
    )
)

tagged_2 = (
    sequenced
    .withColumn("play_device_count_avg_3_minus_last_played_at_ter", F.col("play_device_count_avg_3") - F.col("last_played_at_tercile"))
)

result = (
    tagged_2
    .select(
        "id",
        "signup_date",
        "last_played_at",
        "play_device_count",
        "play_device_count_avg_3",
        "last_played_at_tercile",
        F.to_date(F.col("last_played_at")).alias("last_played_at_date"),
    )
    .orderBy(F.col("last_played_at").desc(), "id")
    .limit(10)
)
