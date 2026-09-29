from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_all_by_is_skipped_flag_full = Window.orderBy("is_skipped_flag", "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_all_by_is_skipped_flag = Window.orderBy("is_skipped_flag", "id")
w_all_by_ms_played_prior_3 = Window.orderBy(F.col("ms_played").asc_nulls_last(), "id").rowsBetween(-3, -1)
w_device = Window.partitionBy("device")
w_device_by_ms_played_vs_devic = Window.partitionBy("device").orderBy("ms_played_vs_device_avg", "id")
w_device_by_ms_played_vs_devic_2 = Window.partitionBy("device").orderBy("ms_played_vs_device_avg", "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)

prepared = (
    df_plays
    .filter((F.col("source").isin("album", "radio", "search")) & (F.col("track_id").isNotNull()))
    .select(
        "id",
        "listener_id",
        "track_id",
        "played_at",
        "ms_played",
        "device",
        "is_skipped",
        "source",
        F.concat_ws("|", F.col("device"), F.col("source")).alias("device_source_label"),
        F.when(F.col("is_skipped"), 1).otherwise(0).alias("is_skipped_flag"),
        F.concat_ws("-", F.col("device"), F.col("source")).alias("device_source_label_2"),
    )
)

with_trends = (
    prepared
    .select(
        "id",
        "track_id",
        "ms_played",
        "device",
        "device_source_label_2",
        F.first("ms_played").over(w_all_by_is_skipped_flag_full).alias("first_ms_played"),
        F.round(F.percent_rank().over(w_all_by_is_skipped_flag), 3).alias("is_skipped_flag_percent_rank"),
        F.round(F.avg("ms_played").over(w_all_by_ms_played_prior_3), 2).alias("ms_played_avg_3_prior"),
        F.round(F.col("ms_played") - F.avg("ms_played").over(w_device), 2).alias("ms_played_vs_device_avg"),
        F.max("is_skipped_flag").over(w_device).alias("device_max_is_skipped_flag"),
        F.count(F.lit(1)).over(w_device).alias("rows_in_device"),
    )
)

enriched = (
    with_trends
    .select(
        "id",
        "ms_played",
        "device",
        "first_ms_played",
        "ms_played_avg_3_prior",
        "ms_played_vs_device_avg",
        "device_max_is_skipped_flag",
        F.lower(F.regexp_replace(F.trim(F.col("device_source_label_2")), " ", "_")).alias("device_source_label_2_slug"),
        F.round(F.col("ms_played_avg_3_prior") * 1.1, 2).alias("ms_played_avg_3_prior_adj"),
        (F.col("first_ms_played") - F.col("ms_played_vs_device_avg")).alias("first_ms_played_minus_ms_played_vs_device_avg"),
    )
)

with_trends_2 = (
    enriched
    .select(
        "*",
        F.lead("first_ms_played").over(w_device_by_ms_played_vs_devic).alias("next_first_ms_played"),
        F.last("ms_played_avg_3_prior").over(w_device_by_ms_played_vs_devic_2).alias("last_ms_played_avg_3_prior"),
        F.ntile(4).over(w_device_by_ms_played_vs_devic).alias("ms_played_vs_device_avg_quartile"),
    )
)

result = (
    with_trends_2
    .groupBy("device_max_is_skipped_flag", "ms_played_vs_device_avg_quartile")
    .pivot("device", ["phone", "tv", "desktop"])
    .agg(F.count("first_ms_played").alias("count_v"), F.sum("first_ms_played").alias("sum_v"))
)
