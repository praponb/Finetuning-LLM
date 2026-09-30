from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_all_by_ms_played_moving_6 = Window.orderBy("ms_played", "id").rowsBetween(-6, Window.currentRow)
w_listener_plan_by_listener_si = Window.partitionBy("listener_plan").orderBy(F.col("listener_signup_date").desc(), "id").rowsBetween(-3, Window.currentRow)
w_listener_plan = Window.partitionBy("listener_plan")
w_listener_id_listener_plan_by = Window.partitionBy("listener_id", "listener_plan").orderBy("listener_signup_date", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_listener_plan_by_listener_si_2 = Window.partitionBy("listener_plan").orderBy(F.col("listener_signup_date").desc(), "id")
w_listener_plan_by_listener_si_3 = Window.partitionBy("listener_plan").orderBy(F.col("listener_signup_date").desc())
w_listener_id_listener_plan_by_2 = Window.partitionBy("listener_id", "listener_plan").orderBy("listener_signup_date", "id")
w_listener_signup_date_tercile = Window.partitionBy("listener_signup_date_tercile").orderBy("distinct_listener_id_in_listener_plan", "id")
w_listener_signup_date_month = Window.partitionBy(F.month("listener_signup_date"))
w_all_by_distinct_listener_id = Window.orderBy("distinct_listener_id_in_listener_plan", "id").rowsBetween(-3, Window.currentRow)

prepared_plays = (
    df_plays
    .filter((F.col("listener_id") >= 14) & (F.col("ms_played") > 112786))
    .select(
        "id",
        "listener_id",
        "track_id",
        "ms_played",
        "device",
        "source",
        F.date_format(F.col("played_at"), "yyyy-MM-dd").alias("played_at_label"),
    )
)

combined_details = (
    prepared_plays.alias("s")
    .join(df_listeners.alias("l"), F.col("l.id") == F.col("s.listener_id"), "left")
    .select(
        "s.id",
        "s.listener_id",
        "s.track_id",
        "s.ms_played",
        "s.source",
        "s.played_at_label",
        F.col("l.plan").alias("listener_plan"),
        F.col("l.signup_date").alias("listener_signup_date"),
    )
)

ranked = (
    combined_details
    .select(
        "id",
        "track_id",
        "source",
        "played_at_label",
        "listener_plan",
        "listener_signup_date",
        F.max("ms_played").over(w_all_by_ms_played_moving_6).alias("ms_played_max_7"),
        F.round(F.avg("ms_played").over(w_listener_plan_by_listener_si), 2).alias("ms_played_avg_4"),
        F.round(F.avg("ms_played").over(w_listener_plan), 2).alias("listener_plan_avg_ms_played"),
        F.last("ms_played", ignorenulls=True).over(w_listener_id_listener_plan_by).alias("last_known_ms_played"),
        F.count(F.lit(1)).over(w_listener_id_listener_plan_by).alias("running_count"),
        F.lead("listener_signup_date", 2).over(w_listener_plan_by_listener_si_2).alias("next2_listener_signup_date"),
        F.round(F.cume_dist().over(w_listener_plan_by_listener_si_3), 3).alias("listener_signup_date_cume_dist"),
        F.rank().over(w_listener_plan_by_listener_si_2).alias("listener_signup_date_rank"),
        F.ntile(3).over(w_listener_id_listener_plan_by_2).alias("listener_signup_date_tercile"),
        F.size(F.collect_set("listener_id").over(w_listener_plan)).alias("distinct_listener_id_in_listener_plan"),
        F.lead("ms_played").over(w_listener_plan_by_listener_si_2).alias("next_ms_played"),
        F.sum("ms_played").over(w_listener_plan).alias("listener_plan_sum_ms_played"),
        F.first("ms_played", ignorenulls=True).over(w_listener_id_listener_plan_by).alias("first_known_ms_played"),
        F.round(F.col("ms_played") / F.nullif(F.sum("ms_played").over(w_listener_plan), F.lit(0)), 4).alias("ms_played_share_of_listener_plan"),
    )
)

first_rows = (
    ranked
    .filter((F.col("listener_signup_date_rank") == 1) & (F.col("ms_played_avg_4") < 301250.0))
    .drop("listener_signup_date_rank")
)

with_trends = (
    first_rows
    .select(
        "id",
        "source",
        "played_at_label",
        "listener_plan",
        "ms_played_max_7",
        "ms_played_avg_4",
        "listener_plan_avg_ms_played",
        "next2_listener_signup_date",
        "listener_signup_date_cume_dist",
        "listener_signup_date_tercile",
        "distinct_listener_id_in_listener_plan",
        "listener_plan_sum_ms_played",
        "first_known_ms_played",
        "ms_played_share_of_listener_plan",
        F.lead("next2_listener_signup_date", 3).over(w_listener_signup_date_tercile).alias("next3_next2_listener_signup_date"),
        F.round(F.avg("listener_plan_sum_ms_played").over(w_listener_signup_date_month), 2).alias("listener_signup_date_month_avg_listener_plan_sum"),
        F.max("listener_signup_date_tercile").over(w_listener_signup_date_month).alias("listener_signup_date_month_max_listener_signup_d"),
        F.max("running_count").over(w_all_by_distinct_listener_id).alias("running_count_max_4"),
    )
)

result = (
    with_trends
    .select(
        "id",
        "played_at_label",
        "listener_plan",
        "ms_played_avg_4",
        "listener_plan_avg_ms_played",
        "next2_listener_signup_date",
        "distinct_listener_id_in_listener_plan",
        "listener_plan_sum_ms_played",
        "ms_played_share_of_listener_plan",
        "next3_next2_listener_signup_date",
        "listener_signup_date_month_avg_listener_plan_sum",
        "running_count_max_4",
        F.greatest(F.col("running_count_max_4"), F.col("ms_played_avg_4")).alias("greatest_running_count_max_4_ms_played_avg_4"),
    )
    .orderBy(
        F.col("running_count_max_4").desc(),
        F.col("distinct_listener_id_in_listener_plan").asc_nulls_last(),
    )
)
