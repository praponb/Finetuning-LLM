from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_latest = Window.partitionBy("device", "source").orderBy(F.col("played_at_plus_2h").desc(), F.col("id").desc())

base = (
    df_plays
    .filter((F.col("is_skipped")) & (F.col("ms_played") > 160264))
    .select(
        "id",
        "listener_id",
        "track_id",
        "played_at",
        "ms_played",
        "device",
        "is_skipped",
        "source",
    )
)

scored = (
    base
    .select(
        "*",
        F.abs(F.col("ms_played") - 160264).alias("ms_played_distance"),
        (F.col("played_at") + F.expr("INTERVAL 2 HOURS")).alias("played_at_plus_2h"),
        F.date_trunc("month", F.col("played_at")).alias("played_at_month"),
    )
)

latest = (
    scored
    .withColumn("rn", F.row_number().over(w_latest))
    .filter(F.col("rn") == 1)
    .drop("rn")
)

qualified = (
    latest.alias("s")
    .join(df_plays.alias("p"), (F.col("p.listener_id") == F.col("s.listener_id")) & (F.col("p.device") == "car") & (~F.col("p.is_skipped")), "left_anti")
)

enriched = (
    qualified
    .filter(F.to_date(F.col("played_at_plus_2h")) < F.lit("2025-02-13").cast("date"))
    .select(
        "*",
        F.when(F.col("source").isin("search", "album"), "group_a").when(F.col("source") == "radio", "group_b").otherwise("other").alias("source_group"),
        (F.col("ms_played") + F.col("ms_played_distance")).alias("ms_played_plus_ms_played_distance"),
    )
)

result = (
    enriched
    .groupBy("played_at_plus_2h", "device")
    .pivot("source_group", ["group_a", "group_b"])
    .agg(F.max("ms_played"))
    .orderBy("played_at_plus_2h", "device")
)
