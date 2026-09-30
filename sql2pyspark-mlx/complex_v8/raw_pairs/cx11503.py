from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_played_at_year_is_skipped_by = Window.partitionBy(F.year("played_at"), "is_skipped").orderBy("ms_played", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_track_id_listener_plan_by_pl = Window.partitionBy("track_id", "listener_plan").orderBy("played_at")
w_source = Window.partitionBy("source")
w_played_at_year_is_skipped_by_2 = Window.partitionBy(F.year("played_at"), "is_skipped").orderBy("ms_played", "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_all_by_played_at_running = Window.orderBy("played_at", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_all_by_played_at_prior_2 = Window.orderBy("played_at", "id").rowsBetween(-2, -1)
w_track_id_listener_plan_by_pl_2 = Window.partitionBy("track_id", "listener_plan").orderBy("played_at", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_all_by_played_at = Window.orderBy("played_at", "id")
w_all_by_played_at_moving_6 = Window.orderBy("played_at", "id").rowsBetween(-6, Window.currentRow)
w_all_by_played_at_ties = Window.orderBy("played_at")
w_all_by_listener_birth_year_m = Window.orderBy("listener_birth_year_max_7", "id")
w_played_at_dense_rank_2_liste = Window.partitionBy("played_at_dense_rank_2", "listener_plan_group").orderBy(F.col("listener_birth_year_share_of_source").desc(), "id")
w_played_at_dense_rank_played = Window.partitionBy("played_at_dense_rank", F.to_date("played_at"))
w_all_by_listener_birth_year_m_2 = Window.orderBy("listener_birth_year_max_7", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_played_at_dense_rank_2_liste_2 = Window.partitionBy("played_at_dense_rank_2", "listener_plan_group").orderBy(F.col("listener_birth_year_share_of_source").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_listener_birth_year_max_7_de = Window.partitionBy("listener_birth_year_max_7_decile").orderBy("distinct_listener_plan_groups", "listener_plan_group", "last_known_played_at_month", "listener_birth_year_max_7_decile")

base_plays = (
    df_plays
    .filter(F.col("device") == "car")
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

combined_details = (
    base_plays.alias("s")
    .join(df_listeners.alias("l"), (F.col("l.id") == F.col("s.listener_id")) & (F.col("l.signup_date").isNotNull()), "inner")
    .select(
        "s.id",
        "s.listener_id",
        "s.track_id",
        "s.played_at",
        "s.ms_played",
        "s.is_skipped",
        "s.source",
        F.col("l.birth_year").alias("listener_birth_year"),
        F.col("l.interests").alias("listener_interests"),
        F.col("l.plan").alias("listener_plan"),
    )
)

ranked = (
    combined_details
    .select(
        "*",
        F.first("listener_birth_year", ignorenulls=True).over(w_played_at_year_is_skipped_by).alias("first_known_listener_birth_year"),
        F.round(F.percent_rank().over(w_track_id_listener_plan_by_pl), 3).alias("played_at_percent_rank"),
        F.size(F.collect_set("listener_plan").over(w_source)).alias("distinct_listener_plan_in_source"),
        F.first("ms_played", ignorenulls=True).over(w_played_at_year_is_skipped_by).alias("first_known_ms_played"),
        F.nth_value("played_at", 2).over(w_played_at_year_is_skipped_by_2).alias("second_played_at"),
        F.count(F.lit(1)).over(w_source).alias("rows_in_source"),
        F.last("played_at", ignorenulls=True).over(w_all_by_played_at_running).alias("last_known_played_at"),
        F.round(F.avg("listener_birth_year").over(w_all_by_played_at_prior_2), 2).alias("listener_birth_year_avg_2_prior"),
        F.first("played_at", ignorenulls=True).over(w_track_id_listener_plan_by_pl_2).alias("first_known_played_at"),
        F.dense_rank().over(w_all_by_played_at).alias("played_at_dense_rank"),
        F.round(F.col("listener_birth_year") / F.nullif(F.sum("listener_birth_year").over(w_source), F.lit(0)), 4).alias("listener_birth_year_share_of_source"),
        F.max("listener_birth_year").over(w_all_by_played_at_moving_6).alias("listener_birth_year_max_7"),
        F.min("listener_birth_year").over(w_source).alias("source_min_listener_birth_year"),
        F.dense_rank().over(w_all_by_played_at_ties).alias("played_at_dense_rank_2"),
    )
)

derived = (
    ranked
    .select(
        "id",
        "played_at",
        "ms_played",
        "listener_birth_year",
        "distinct_listener_plan_in_source",
        "first_known_ms_played",
        "second_played_at",
        "rows_in_source",
        "last_known_played_at",
        "first_known_played_at",
        "played_at_dense_rank",
        "listener_birth_year_share_of_source",
        "listener_birth_year_max_7",
        "source_min_listener_birth_year",
        "played_at_dense_rank_2",
        F.when(F.col("listener_plan").isin("family", "free"), "group_a").when(F.col("listener_plan") == "student", "group_b").otherwise("other").alias("listener_plan_group"),
    )
)

sequenced = (
    derived
    .select(
        "*",
        F.lag("first_known_played_at", 3).over(w_all_by_listener_birth_year_m).alias("prev3_first_known_played_at"),
        F.round(F.percent_rank().over(w_played_at_dense_rank_2_liste), 3).alias("listener_birth_year_share_of_source_percent_rank"),
        F.count(F.lit(1)).over(w_played_at_dense_rank_played).alias("rows_in_played_at_dense_rank_played_at_to_date"),
        F.ntile(10).over(w_all_by_listener_birth_year_m).alias("listener_birth_year_max_7_decile"),
        F.round(F.col("first_known_ms_played") - F.avg("first_known_ms_played").over(w_played_at_dense_rank_played), 2).alias("first_known_ms_played_vs_played_at_dense_rank_pl"),
        F.last("first_known_ms_played", ignorenulls=True).over(w_all_by_listener_birth_year_m_2).alias("last_known_first_known_ms_played"),
        F.rank().over(w_all_by_listener_birth_year_m).alias("listener_birth_year_max_7_rank"),
        F.min("ms_played").over(w_played_at_dense_rank_2_liste_2).alias("min_ms_played_so_far"),
    )
)

grouped = (
    sequenced
    .groupBy(
        "listener_plan_group",
        F.date_trunc("month", "last_known_played_at").alias("last_known_played_at_month"),
        "listener_birth_year_max_7_decile",
    )
    .agg(
        F.countDistinct("listener_plan_group").alias("distinct_listener_plan_groups"),
        F.count("*").alias("cnt"),
        F.min("last_known_played_at").alias("first_last_known_played_at"),
    )
)

windowed = (
    grouped
    .select(
        "listener_plan_group",
        "last_known_played_at_month",
        "listener_birth_year_max_7_decile",
        "distinct_listener_plan_groups",
        F.round((F.unix_timestamp("first_last_known_played_at") - F.unix_timestamp(F.lag("first_last_known_played_at").over(w_listener_birth_year_max_7_de))) / 60, 1).alias("minutes_since_prev_first_last_known_played_at"),
        F.ntile(4).over(w_listener_birth_year_max_7_de).alias("distinct_listener_plan_groups_quartile"),
        F.row_number().over(w_listener_birth_year_max_7_de).alias("rn"),
    )
)

result = (
    windowed
    .select(
        "listener_plan_group",
        "last_known_played_at_month",
        "minutes_since_prev_first_last_known_played_at",
        "rn",
        F.concat(F.col("listener_plan_group"), F.lit("#"), F.col("rn").cast("string")).alias("listener_plan_group_ref"),
    )
    .orderBy(F.col("last_known_played_at_month").asc(), "rn")
)
