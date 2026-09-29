from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_listener_plan_by_played_at_d = Window.partitionBy("listener_plan").orderBy("played_at_day", "id")
w_device_played_at_day_year_by = Window.partitionBy("device", F.year("played_at_day")).orderBy(F.col("played_at_day").desc())
w_all_by_played_at_day_ties = Window.orderBy("played_at_day")
w_all_by_listener_signup_date = Window.orderBy("listener_signup_date", "id")
w_device_played_at_day_year_by_2 = Window.partitionBy("device", F.year("played_at_day")).orderBy(F.col("played_at_day").desc(), "id")
w_listener_plan_by_played_at_f = Window.partitionBy("listener_plan").orderBy(F.col("played_at").desc_nulls_first(), "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_listener_plan_by_played_at = Window.partitionBy("listener_plan").orderBy(F.col("played_at").desc_nulls_first(), "id")
w_device_track_id_by_played_at = Window.partitionBy("device", "track_id").orderBy("played_at", "id")
w_device_played_at_day_year_by_3 = Window.partitionBy("device", F.year("played_at_day")).orderBy(F.col("played_at_day").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_track_id_played_at_month = Window.partitionBy("track_id", F.date_trunc("month", "played_at"))
w_all_by_first_played_at_runni = Window.orderBy("first_played_at", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_all_by_prev2_listener_signup = Window.orderBy("prev2_listener_signup_date", "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_all_by_first_played_at = Window.orderBy("first_played_at", "id")
w_first_played_at_quartile_pla = Window.partitionBy("first_played_at_quartile", "played_at_day_rank").orderBy("total_first_played_at_quartile")
w_first_played_at_quartile_pla_2 = Window.partitionBy("first_played_at_quartile", "played_at_day_rank").orderBy("total_first_played_at_quartile", "played_at_day_rank", "listener_signup_date_month_end_ym", "first_played_at_quartile").rowsBetween(-2, -1)
w_first_played_at_quartile = Window.partitionBy("first_played_at_quartile")

filtered = (
    df_plays
    .filter((F.col("is_skipped")) & (F.col("track_id").isNotNull()))
    .select(
        "id",
        "listener_id",
        "track_id",
        "played_at",
        "ms_played",
        "device",
        "is_skipped",
        F.date_trunc("day", F.col("played_at")).alias("played_at_day"),
    )
)

joined = (
    filtered.alias("s")
    .join(df_listeners.alias("l"), F.col("l.id") == F.col("s.listener_id"), "left")
    .select(
        "s.id",
        "s.listener_id",
        "s.track_id",
        "s.played_at",
        "s.device",
        "s.is_skipped",
        "s.played_at_day",
        F.col("l.country").alias("listener_country"),
        F.col("l.signup_date").alias("listener_signup_date"),
        F.col("l.plan").alias("listener_plan"),
    )
)

with_trends = (
    joined
    .select(
        "*",
        F.round((F.unix_timestamp("played_at_day") - F.unix_timestamp(F.lag("played_at_day").over(w_listener_plan_by_played_at_d))) / 60, 1).alias("minutes_since_prev_played_at_day"),
        F.round(F.percent_rank().over(w_device_played_at_day_year_by), 3).alias("played_at_day_percent_rank"),
        F.round(F.cume_dist().over(w_all_by_played_at_day_ties), 3).alias("played_at_day_cume_dist"),
        F.lead("played_at", 2).over(w_all_by_listener_signup_date).alias("next2_played_at"),
        F.lead("played_at_day").over(w_device_played_at_day_year_by_2).alias("next_played_at_day"),
        F.first("played_at").over(w_listener_plan_by_played_at_f).alias("first_played_at"),
        F.dense_rank().over(w_listener_plan_by_played_at).alias("played_at_dense_rank"),
        F.ntile(3).over(w_all_by_listener_signup_date).alias("listener_signup_date_tercile"),
        F.lag("listener_signup_date", 3).over(w_device_track_id_by_played_at).alias("prev3_listener_signup_date"),
        F.lead("listener_signup_date").over(w_all_by_listener_signup_date).alias("next_listener_signup_date"),
        F.rank().over(w_listener_plan_by_played_at_d).alias("played_at_day_rank"),
        F.lag("listener_signup_date", 2).over(w_all_by_listener_signup_date).alias("prev2_listener_signup_date"),
        F.last("listener_signup_date", ignorenulls=True).over(w_device_played_at_day_year_by_3).alias("last_known_listener_signup_date"),
    )
)

tagged = (
    with_trends
    .filter(F.hour(F.col("first_played_at")).between(8, 10))
    .select(
        "id",
        "listener_id",
        "track_id",
        "played_at",
        "device",
        "is_skipped",
        "played_at_day",
        "minutes_since_prev_played_at_day",
        "played_at_day_percent_rank",
        "played_at_day_cume_dist",
        "next2_played_at",
        "next_played_at_day",
        "first_played_at",
        "played_at_dense_rank",
        "listener_signup_date_tercile",
        "next_listener_signup_date",
        "played_at_day_rank",
        "prev2_listener_signup_date",
        F.last_day(F.col("listener_signup_date")).alias("listener_signup_date_month_end"),
    )
)

windowed = (
    tagged
    .select(
        "id",
        "listener_id",
        "played_at",
        "device",
        "is_skipped",
        "played_at_day",
        "minutes_since_prev_played_at_day",
        "played_at_day_percent_rank",
        "next2_played_at",
        "next_played_at_day",
        "first_played_at",
        "played_at_dense_rank",
        "played_at_day_rank",
        "listener_signup_date_month_end",
        F.count(F.lit(1)).over(w_track_id_played_at_month).alias("rows_in_track_id_played_at_month"),
        F.round(F.col("played_at_day_cume_dist") / F.nullif(F.sum("played_at_day_cume_dist").over(w_track_id_played_at_month), F.lit(0)), 4).alias("played_at_day_cume_dist_share_of_track_id_played"),
        F.sum("listener_signup_date_tercile").over(w_all_by_first_played_at_runni).alias("running_listener_signup_date_tercile"),
        F.size(F.collect_set("listener_id").over(w_track_id_played_at_month)).alias("distinct_listener_id_in_track_id_played_at_month"),
        F.nth_value("next_listener_signup_date", 2).over(w_all_by_prev2_listener_signup).alias("second_next_listener_signup_date"),
        F.ntile(4).over(w_all_by_first_played_at).alias("first_played_at_quartile"),
    )
)

stats = (
    windowed
    .groupBy(
        "played_at_day_rank",
        F.date_format("listener_signup_date_month_end", "yyyy-MM").alias("listener_signup_date_month_end_ym"),
        "first_played_at_quartile",
    )
    .agg(
        F.sum("first_played_at_quartile").alias("total_first_played_at_quartile"),
        F.min("played_at").alias("first_played_at_2"),
        F.array_sort(F.collect_set("device")).alias("device_list"),
    )
)

sequenced = (
    stats
    .select(
        "*",
        F.round(F.cume_dist().over(w_first_played_at_quartile_pla), 3).alias("total_first_played_at_quartile_cume_dist"),
        F.round(F.avg("first_played_at_quartile").over(w_first_played_at_quartile_pla_2), 2).alias("first_played_at_quartile_avg_2_prior"),
        F.round(F.col("first_played_at_quartile") - F.avg("first_played_at_quartile").over(w_first_played_at_quartile), 2).alias("first_played_at_quartile_vs_first_played_at_quar"),
    )
)

result = (
    sequenced
    .select(
        "played_at_day_rank",
        "listener_signup_date_month_end_ym",
        "first_played_at_quartile",
        "total_first_played_at_quartile",
        "device_list",
        "first_played_at_quartile_avg_2_prior",
        "first_played_at_quartile_vs_first_played_at_quar",
        F.when(F.col("first_played_at_quartile") >= 8, "large").when(F.col("first_played_at_quartile") >= 5, "medium").otherwise("small").alias("first_played_at_quartile_tier"),
        (F.floor(F.col("first_played_at_quartile") / 2) * 2).alias("first_played_at_quartile_bucket"),
    )
    .orderBy(F.col("first_played_at_quartile_bucket").desc_nulls_first())
)
