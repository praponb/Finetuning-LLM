from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_played_at_year_by_listener_s = Window.partitionBy(F.year("played_at")).orderBy("listener_signup_date", "id").rowsBetween(-2, Window.currentRow)
w_device_track_id = Window.partitionBy("device", "track_id")
w_listener_signup_date_month_l = Window.partitionBy(F.month("listener_signup_date"), "listener_country").orderBy("ms_played_bucket", "id")
w_listener_id_by_ms_played_buc = Window.partitionBy("listener_id").orderBy(F.col("ms_played_bucket").asc_nulls_last(), "id").rowsBetween(-6, Window.currentRow)
w_played_at_year_by_listener_s_2 = Window.partitionBy(F.year("played_at")).orderBy("listener_signup_date", "id")
w_played_at_year_by_listener_s_3 = Window.partitionBy(F.year("played_at")).orderBy("listener_signup_date")
w_listener_id_by_ms_played_buc_2 = Window.partitionBy("listener_id").orderBy(F.col("ms_played_bucket").asc_nulls_last(), "id").rowsBetween(-6, -1)
w_listener_id_by_ms_played_buc_3 = Window.partitionBy("listener_id").orderBy(F.col("ms_played_bucket").asc_nulls_last())
w_listener_signup_date_month_l_2 = Window.partitionBy(F.month("listener_signup_date"), "listener_country").orderBy("ms_played_bucket")

base_plays = (
    df_plays
    .filter(F.col("ms_played").between(72464, 230798))
    .select(
        "id",
        "listener_id",
        "track_id",
        "played_at",
        "device",
        "is_skipped",
        F.initcap(F.trim(F.col("source"))).alias("source_clean"),
        (F.floor(F.col("ms_played") / 50000) * 50000).alias("ms_played_bucket"),
    )
)

joined = (
    base_plays.alias("s")
    .join(df_listeners.alias("l"), F.col("l.id") == F.col("s.listener_id"), "inner")
    .select(
        "s.*",
        F.col("l.country").alias("listener_country"),
        F.col("l.signup_date").alias("listener_signup_date"),
        F.col("l.plan").alias("listener_plan"),
    )
)

windowed = (
    joined
    .select(
        "id",
        "listener_id",
        "track_id",
        "device",
        "source_clean",
        "listener_country",
        "listener_plan",
        F.round(F.avg("ms_played_bucket").over(w_played_at_year_by_listener_s), 2).alias("ms_played_bucket_avg_3"),
        F.round(F.col("ms_played_bucket") - F.avg("ms_played_bucket").over(w_device_track_id), 2).alias("ms_played_bucket_vs_device_track_id_avg"),
        F.dense_rank().over(w_listener_signup_date_month_l).alias("ms_played_bucket_dense_rank"),
        F.max("ms_played_bucket").over(w_listener_id_by_ms_played_buc).alias("ms_played_bucket_max_7"),
        F.row_number().over(w_listener_signup_date_month_l).alias("ms_played_bucket_seq"),
        F.round(F.percent_rank().over(w_played_at_year_by_listener_s_2), 3).alias("listener_signup_date_percent_rank"),
        F.rank().over(w_played_at_year_by_listener_s_3).alias("listener_signup_date_rank"),
        F.sum("ms_played_bucket").over(w_listener_id_by_ms_played_buc_2).alias("ms_played_bucket_sum_6_prior"),
        F.round(F.cume_dist().over(w_listener_id_by_ms_played_buc_3), 3).alias("ms_played_bucket_cume_dist"),
        F.min("ms_played_bucket").over(w_device_track_id).alias("device_track_id_min_ms_played_bucket"),
        F.lag("played_at", 3).over(w_listener_signup_date_month_l).alias("prev3_played_at"),
        F.rank().over(w_listener_signup_date_month_l_2).alias("ms_played_bucket_rank"),
        F.dense_rank().over(w_listener_signup_date_month_l_2).alias("ms_played_bucket_dense_rank_2"),
        F.round(F.col("ms_played_bucket") / F.nullif(F.sum("ms_played_bucket").over(w_device_track_id), F.lit(0)), 4).alias("ms_played_bucket_share_of_device_track_id"),
    )
)

stats = (
    windowed
    .groupBy("listener_plan")
    .agg(
        F.percentile_approx("ms_played_bucket_max_7", 0.5).alias("median_ms_played_bucket_max_7"),
        F.min("ms_played_bucket_dense_rank").alias("min_ms_played_bucket_dense_rank"),
        F.countDistinct("listener_plan").alias("distinct_listener_plans"),
        F.sum(F.when(F.col("listener_country") != "JP", F.col("device_track_id_min_ms_played_bucket")).otherwise(0)).alias("device_track_id_min_ms_played_bucket_listener_co"),
        F.array_sort(F.collect_set("listener_country")).alias("listener_country_list"),
    )
)

result = (
    stats
    .select(
        "listener_plan",
        "median_ms_played_bucket_max_7",
        "min_ms_played_bucket_dense_rank",
        F.lower(F.regexp_replace(F.trim(F.col("listener_plan")), " ", "_")).alias("listener_plan_slug"),
    )
    .orderBy("listener_plan", F.col("median_ms_played_bucket_max_7").desc())
)
