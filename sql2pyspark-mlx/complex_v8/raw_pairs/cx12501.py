from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_dedup = Window.partitionBy("source", "device_group_group").orderBy("played_at", "id")

base = (
    df_plays
    .filter((~F.col("source").isin("playlist", "radio")) & (F.col("listener_id") <= 13))
    .select(
        "id",
        "listener_id",
        "track_id",
        "played_at",
        "is_skipped",
        "source",
        F.when(F.col("device").isin("tv", "speaker"), "group_a").when(F.col("device") == "desktop", "group_b").otherwise("other").alias("device_group"),
        F.when(F.col("device").isin("car", "speaker"), "group_a").when(F.col("device") == "tv", "group_b").otherwise("other").alias("device_group_2"),
        F.date_format(F.col("played_at"), "yyyy-MM-dd HH:00").alias("played_at_label"),
    )
)

scored = (
    base
    .filter(F.col("track_id") >= 16)
    .select(
        "*",
        F.when(F.col("device_group").isin("other", "group_b"), "group_a").when(F.col("device_group") == "group_a", "group_b").otherwise("other").alias("device_group_group"),
        F.when(F.col("is_skipped"), "yes").otherwise("no").alias("is_skipped_label"),
        F.lower(F.regexp_replace(F.trim(F.col("played_at_label")), " ", "_")).alias("played_at_label_slug"),
    )
)

latest = (
    scored
    .withColumn("recency_rank", F.row_number().over(w_dedup))
    .filter(F.col("recency_rank") == 1)
    .drop("recency_rank")
)

result = (
    latest
    .filter(F.col("device_group_2").isin("group_b", "group_a"))
    .select(
        "id",
        "listener_id",
        "track_id",
        "played_at",
        "is_skipped",
        "source",
        "device_group",
        "device_group_2",
        "device_group_group",
        F.length(F.trim(F.col("played_at_label"))).alias("played_at_label_len"),
        F.when(F.col("is_skipped"), "yes").otherwise("no").alias("is_skipped_label_2"),
    )
    .orderBy(F.col("id").desc_nulls_first(), F.col("device_group_2").desc())
    .limit(10)
)
