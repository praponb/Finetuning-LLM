from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_all_by_played_at_running = Window.orderBy("played_at", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_all_by_played_at = Window.orderBy("played_at", "id")

scoped_plays = (
    df_plays
    .filter(~F.col("is_skipped"))
    .select(
        "id",
        "listener_id",
        "track_id",
        "played_at",
        "device",
        "source",
    )
)

windowed = (
    scoped_plays
    .withColumn("last_known_played_at", F.last("played_at", ignorenulls=True).over(w_all_by_played_at_running))
    .withColumn("played_at_quintile", F.ntile(5).over(w_all_by_played_at))
)

rollup_base = (
    windowed
    .groupBy(
        F.date_trunc("month", "last_known_played_at").alias("last_known_played_at_month"),
        "source",
        "device",
    )
    .agg(
        F.percentile_approx("played_at_quintile", 0.5).alias("median_played_at_quintile"),
        F.round(F.avg("played_at_quintile"), 2).alias("avg_played_at_quintile"),
        F.max("played_at_quintile").alias("max_played_at_quintile_check"),
    )
    .filter(F.col("max_played_at_quintile_check") > 5)
    .drop("max_played_at_quintile_check")
)

result = (
    rollup_base
    .filter(F.col("device").isNotNull())
    .select(
        "last_known_played_at_month",
        "source",
        "device",
        "median_played_at_quintile",
        "avg_played_at_quintile",
    )
    .orderBy("avg_played_at_quintile")
)
