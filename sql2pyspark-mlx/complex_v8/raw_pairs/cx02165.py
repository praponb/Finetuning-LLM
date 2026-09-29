from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_distinct_devices_by_max_ms_p = Window.partitionBy("distinct_devices").orderBy("max_ms_played", "source", "is_skipped")
w_is_skipped_source = Window.partitionBy("is_skipped", "source")
w_cnt_is_skipped_by_distinct_d = Window.partitionBy("cnt", "is_skipped").orderBy(F.col("distinct_devices").desc_nulls_first(), "source", "is_skipped")
w_cnt_max_ms_played_rank_by_is = Window.partitionBy("cnt", "max_ms_played_rank").orderBy(F.col("is_skipped_source_avg_max_ms_played").desc(), "source", "is_skipped")
w_cnt_max_ms_played_rank_by_ma = Window.partitionBy("cnt", "max_ms_played_rank").orderBy("max_ms_played")
w_cnt_max_ms_played_rank_by_ma_2 = Window.partitionBy("cnt", "max_ms_played_rank").orderBy("max_ms_played", "source", "is_skipped").rowsBetween(-2, -1)
w_is_skipped_max_ms_played_ran = Window.partitionBy("is_skipped", "max_ms_played_rank")

prepared_plays = (
    df_plays
    .filter(F.col("ms_played") >= 73335)
    .select(
        "id",
        "listener_id",
        "track_id",
        "played_at",
        "ms_played",
        "device",
        "is_skipped",
        "source",
        F.lower(F.regexp_replace(F.trim(F.col("source")), " ", "_")).alias("source_slug"),
        F.coalesce(F.col("source"), F.lit("UNKNOWN")).alias("source_or_unknown"),
        F.initcap(F.trim(F.col("source"))).alias("source_clean"),
    )
)

without_matches = (
    prepared_plays
    .crossJoin(prepared_plays.agg(F.avg("ms_played").alias("overall_avg_ms_played")))
    .filter(F.col("ms_played") > F.col("overall_avg_ms_played"))
    .drop("overall_avg_ms_played")
)

rollup_base = (
    without_matches
    .groupBy("source", "is_skipped")
    .agg(
        F.count("*").alias("cnt"),
        F.sum("ms_played").alias("total_ms_played"),
        F.countDistinct("device").alias("distinct_devices"),
        F.max("ms_played").alias("max_ms_played"),
    )
    .filter(F.col("max_ms_played") > 129336)
)

with_activity = (
    rollup_base
    .crossJoin(rollup_base.agg(F.avg("cnt").alias("overall_avg_cnt")))
    .filter(F.col("cnt") > F.col("overall_avg_cnt"))
    .drop("overall_avg_cnt")
)

windowed = (
    with_activity
    .select(
        "source",
        "is_skipped",
        "cnt",
        "max_ms_played",
        F.rank().over(w_distinct_devices_by_max_ms_p).alias("max_ms_played_rank"),
        F.max("cnt").over(w_is_skipped_source).alias("is_skipped_source_max_cnt"),
        F.round(F.avg("max_ms_played").over(w_is_skipped_source), 2).alias("is_skipped_source_avg_max_ms_played"),
        F.dense_rank().over(w_cnt_is_skipped_by_distinct_d).alias("distinct_devices_dense_rank"),
    )
)

windowed_2 = (
    windowed
    .select(
        "*",
        F.ntile(10).over(w_cnt_max_ms_played_rank_by_is).alias("is_skipped_source_avg_max_ms_played_decile"),
        F.dense_rank().over(w_cnt_max_ms_played_rank_by_ma).alias("max_ms_played_dense_rank"),
        F.max("max_ms_played").over(w_cnt_max_ms_played_rank_by_ma_2).alias("max_ms_played_max_2_prior"),
        F.round(F.cume_dist().over(w_cnt_max_ms_played_rank_by_ma), 3).alias("max_ms_played_cume_dist"),
        F.rank().over(w_cnt_max_ms_played_rank_by_ma).alias("max_ms_played_rank_2"),
        F.round(F.col("is_skipped_source_max_cnt") / F.nullif(F.sum("is_skipped_source_max_cnt").over(w_is_skipped_max_ms_played_ran), F.lit(0)), 4).alias("is_skipped_source_max_cnt_share_of_is_skipped_ma"),
    )
)

result = (
    windowed_2
    .rollup("max_ms_played_dense_rank", "is_skipped", "max_ms_played_rank_2")
    .agg(
        F.round(F.avg("max_ms_played_cume_dist"), 2).alias("avg_max_ms_played_cume_dist"),
        F.count("*").alias("cnt_2"),
        F.countDistinct("source").alias("distinct_sources"),
    )
    .orderBy(
        F.col("max_ms_played_dense_rank").asc_nulls_last(),
        F.col("is_skipped").asc_nulls_last(),
        F.col("max_ms_played_rank_2").asc_nulls_last(),
    )
)
