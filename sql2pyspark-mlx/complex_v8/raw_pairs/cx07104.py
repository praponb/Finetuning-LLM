from pyspark.sql import functions as F

prepared = (
    df_plays
    .filter(F.month(F.col("played_at")).isin(2, 3))
    .select(
        "id",
        "listener_id",
        "track_id",
        "ms_played",
        "device",
        "is_skipped",
        "source",
    )
)

labeled = (
    prepared
    .filter(F.col("ms_played") < 221269)
    .select(
        "*",
        F.when(F.col("device").isin("car", "desktop"), "group_a").when(F.col("device") == "phone", "group_b").otherwise("other").alias("device_group"),
        F.initcap(F.trim(F.col("source"))).alias("source_clean"),
        F.when(F.col("is_skipped"), "yes").otherwise("no").alias("is_skipped_label"),
    )
)

result = (
    labeled
    .groupBy("is_skipped_label")
    .pivot("device_group", ["other", "group_a", "group_b"])
    .agg(F.max("ms_played"))
)
