from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_dedup = Window.partitionBy("listener_id").orderBy(F.col("ms_played").desc(), F.col("id").desc())
w_ms_played_tier_played_at_day = Window.partitionBy("ms_played_tier", F.date_trunc("month", "played_at_day")).orderBy("ms_played")
w_source = Window.partitionBy("source")
w_ms_played_rank_track_id_by_g = Window.partitionBy("ms_played_rank", "track_id").orderBy(F.col("greatest_ms_played_rank_ms_played_vs_source_avg").desc_nulls_first(), "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_device_group = Window.partitionBy("device_group")
w_ms_played_rank_track_id_by_g_2 = Window.partitionBy("ms_played_rank", "track_id").orderBy(F.col("greatest_ms_played_rank_ms_played_vs_source_avg").desc_nulls_first())
w_all_by_ms_played_tier_len = Window.orderBy(F.col("ms_played_tier_len").desc(), "id")

base = (
    df_plays
    .filter(~F.col("is_skipped"))
    .select(
        "id",
        "listener_id",
        "track_id",
        "ms_played",
        "device",
        "is_skipped",
        "source",
        F.date_trunc("day", F.col("played_at")).alias("played_at_day"),
        F.when(F.col("device").isin("speaker", "tv"), "group_a").when(F.col("device") == "desktop", "group_b").otherwise("other").alias("device_group"),
        F.when(F.col("ms_played") >= 308104, "A").when(F.col("ms_played") >= 129336, "B").otherwise("C").alias("ms_played_tier"),
    )
)

deduped = (
    base
    .withColumn("rn", F.row_number().over(w_dedup))
    .filter(F.col("rn") == 1)
    .drop("rn")
)

metrics = (
    deduped
    .withColumn("ms_played_rank", F.rank().over(w_ms_played_tier_played_at_day))
    .withColumn("ms_played_vs_source_avg", F.round(F.col("ms_played") - F.avg("ms_played").over(w_source), 2))
)

derived = (
    metrics
    .withColumn("ms_played_rank_tier", F.when(F.col("ms_played_rank") >= 8, "A").when(F.col("ms_played_rank") >= 4, "B").otherwise("C"))
)

tagged = (
    derived
    .filter(F.col("listener_id").isNotNull())
    .select(
        "*",
        F.round(F.col("ms_played_vs_source_avg") * 1.07, 2).alias("ms_played_vs_source_avg_adj"),
        F.length(F.trim(F.col("ms_played_tier"))).alias("ms_played_tier_len"),
        F.greatest(F.col("ms_played_rank"), F.col("ms_played_vs_source_avg")).alias("greatest_ms_played_rank_ms_played_vs_source_avg"),
    )
)

with_trends = (
    tagged
    .select(
        "*",
        F.min("ms_played_vs_source_avg").over(w_ms_played_rank_track_id_by_g).alias("min_ms_played_vs_source_avg_so_far"),
        F.round(F.avg("ms_played_vs_source_avg").over(w_device_group), 2).alias("device_group_avg_ms_played_vs_source_avg"),
        F.round(F.col("ms_played") / F.nullif(F.sum("ms_played").over(w_device_group), F.lit(0)), 4).alias("ms_played_share_of_device_group"),
        F.rank().over(w_ms_played_rank_track_id_by_g_2).alias("greatest_ms_played_rank_ms_played_vs_source_avg_"),
        F.round(F.cume_dist().over(w_all_by_ms_played_tier_len), 3).alias("ms_played_tier_len_cume_dist"),
    )
)

result = (
    with_trends
    .groupingSets(
        [["is_skipped", "greatest_ms_played_rank_ms_played_vs_source_avg_", "ms_played_rank"], ["is_skipped"], ["greatest_ms_played_rank_ms_played_vs_source_avg_"], []],
        "is_skipped", "greatest_ms_played_rank_ms_played_vs_source_avg_", "ms_played_rank",
    )
    .agg(F.countDistinct("ms_played_tier").alias("distinct_ms_played_tiers"))
    .orderBy(
        F.col("is_skipped").asc_nulls_last(),
        F.col("greatest_ms_played_rank_ms_played_vs_source_avg_").asc_nulls_last(),
        F.col("ms_played_rank").asc_nulls_last(),
    )
)
