from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_listener_signup_date_year_by = Window.partitionBy(F.year("listener_signup_date")).orderBy(F.col("played_at").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_listener_signup_date_year = Window.partitionBy(F.year("listener_signup_date"))
w_listener_signup_date_year_by_2 = Window.partitionBy(F.year("listener_signup_date")).orderBy(F.col("played_at").desc(), "id").rowsBetween(-2, Window.currentRow)
w_listener_signup_date_year_by_3 = Window.partitionBy(F.year("listener_signup_date")).orderBy(F.col("played_at").desc(), "id")
w_device_is_skipped_by_listene = Window.partitionBy("device", "is_skipped").orderBy(F.col("listener_signup_date").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_device_is_skipped_by_listene_2 = Window.partitionBy("device", "is_skipped").orderBy(F.col("listener_signup_date").desc())
w_device_is_skipped_by_listene_3 = Window.partitionBy("device", "is_skipped").orderBy(F.col("listener_signup_date").desc(), "id")
w_all_by_listener_signup_date = Window.orderBy(F.col("listener_signup_date").asc_nulls_last(), "id").rowsBetween(-3, Window.currentRow)
w_all_by_listener_signup_date_2 = Window.orderBy(F.col("listener_signup_date").asc_nulls_last(), "id")
w_is_skipped_by_last_listener = Window.partitionBy("is_skipped").orderBy("last_listener_signup_date", "id")
w_seq_no = Window.partitionBy("seq_no")
w_all_by_ms_played = Window.orderBy(F.col("ms_played").asc_nulls_last(), "id")

valid_plays = df_plays.filter(F.col("is_skipped"))

with_parent = (
    valid_plays.alias("s")
    .join(df_listeners.alias("l"), (F.col("l.id") == F.col("s.listener_id")) & (F.col("l.country") != "JP"), "left")
    .select(
        "s.id",
        "s.listener_id",
        "s.track_id",
        "s.played_at",
        "s.ms_played",
        "s.device",
        "s.is_skipped",
        F.col("l.signup_date").alias("listener_signup_date"),
        F.col("l.plan").alias("listener_plan"),
    )
)

with_windows = (
    with_parent
    .select(
        "*",
        F.first("listener_signup_date", ignorenulls=True).over(w_listener_signup_date_year_by).alias("first_known_listener_signup_date"),
        F.max("ms_played").over(w_listener_signup_date_year).alias("listener_signup_date_year_max_ms_played"),
        F.max("ms_played").over(w_listener_signup_date_year_by_2).alias("ms_played_max_3"),
        F.round(F.col("ms_played") / F.nullif(F.sum("ms_played").over(w_listener_signup_date_year), F.lit(0)), 4).alias("ms_played_share_of_listener_signup_date_year"),
        F.ntile(3).over(w_listener_signup_date_year_by_3).alias("played_at_tercile"),
        F.last("listener_signup_date").over(w_device_is_skipped_by_listene).alias("last_listener_signup_date"),
        F.dense_rank().over(w_device_is_skipped_by_listene_2).alias("listener_signup_date_dense_rank"),
        F.count(F.lit(1)).over(w_listener_signup_date_year).alias("rows_in_listener_signup_date_year"),
        F.row_number().over(w_device_is_skipped_by_listene_3).alias("listener_signup_date_seq"),
        F.lead("played_at").over(w_device_is_skipped_by_listene_3).alias("next_played_at"),
        F.sum("ms_played").over(w_all_by_listener_signup_date).alias("ms_played_sum_4"),
        F.row_number().over(w_all_by_listener_signup_date_2).alias("seq_no"),
        F.min("ms_played").over(w_listener_signup_date_year).alias("listener_signup_date_year_min_ms_played"),
        F.size(F.collect_set("track_id").over(w_listener_signup_date_year)).alias("distinct_track_id_in_listener_signup_date_year"),
    )
)

top_per_group = (
    with_windows
    .filter(F.col("listener_signup_date_seq") <= 3)
    .drop("listener_signup_date_seq")
)

ranked = (
    top_per_group
    .select(
        "id",
        "track_id",
        "played_at",
        "is_skipped",
        "listener_signup_date",
        "listener_plan",
        "first_known_listener_signup_date",
        "listener_signup_date_year_max_ms_played",
        "ms_played_share_of_listener_signup_date_year",
        "listener_signup_date_dense_rank",
        "rows_in_listener_signup_date_year",
        "ms_played_sum_4",
        "listener_signup_date_year_min_ms_played",
        "distinct_track_id_in_listener_signup_date_year",
        F.row_number().over(w_is_skipped_by_last_listener).alias("last_listener_signup_date_seq"),
        F.sum("ms_played_max_3").over(w_seq_no).alias("seq_no_sum_ms_played_max_3"),
        F.row_number().over(w_all_by_ms_played).alias("ms_played_seq"),
    )
)

totals = (
    ranked
    .filter(F.col("last_listener_signup_date_seq") > 2)
    .groupBy("distinct_track_id_in_listener_signup_date_year")
    .agg(
        F.count("*").alias("cnt"),
        F.countDistinct("listener_plan").alias("distinct_listener_plans"),
        F.sum("seq_no_sum_ms_played_max_3").alias("total_seq_no_sum_ms_played_max_3"),
        F.array_sort(F.collect_set("listener_plan")).alias("listener_plan_list"),
        F.countDistinct("track_id").alias("distinct_track_ids"),
    )
)

result = (
    totals
    .select(
        "distinct_track_id_in_listener_signup_date_year",
        "distinct_listener_plans",
        "total_seq_no_sum_ms_played_max_3",
        "listener_plan_list",
    )
    .orderBy(F.col("distinct_track_id_in_listener_signup_date_year").desc())
)
