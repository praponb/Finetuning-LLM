from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_listener_id_by_listener_sign = Window.partitionBy("listener_id").orderBy("listener_signup_date", "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_listener_id_by_listener_sign_2 = Window.partitionBy("listener_id").orderBy("listener_signup_date", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_all_by_played_at = Window.orderBy("played_at", "id")
w_listener_id_listener_plan = Window.partitionBy("listener_id", "listener_plan")
w_device_by_ms_played_bucket_f = Window.partitionBy("device").orderBy(F.col("ms_played_bucket").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_listener_id_by_listener_sign_3 = Window.partitionBy("listener_id").orderBy("listener_signup_date", "id").rowsBetween(-3, Window.currentRow)
w_listener_signup_date_year = Window.partitionBy(F.year("listener_signup_date"))

prepared = (
    df_plays
    .filter(~F.col("device").isin("tv", "car"))
    .select(
        "id",
        "listener_id",
        "track_id",
        "played_at",
        "ms_played",
        "device",
        "is_skipped",
        (F.floor(F.col("ms_played") / 50000) * 50000).alias("ms_played_bucket"),
    )
)

combined_details = (
    prepared.alias("s")
    .join(df_listeners.alias("l"), F.col("l.id") == F.col("s.listener_id"), "left")
    .select(
        "s.id",
        "s.listener_id",
        "s.track_id",
        "s.played_at",
        "s.device",
        "s.ms_played_bucket",
        F.col("l.plan").alias("listener_plan"),
        F.col("l.signup_date").alias("listener_signup_date"),
        F.col("l.username").alias("listener_username"),
    )
)

windowed = (
    combined_details
    .select(
        "*",
        F.nth_value("listener_signup_date", 2).over(w_listener_id_by_listener_sign).alias("second_listener_signup_date"),
        F.max("ms_played_bucket").over(w_listener_id_by_listener_sign_2).alias("max_ms_played_bucket_so_far"),
        F.lag("ms_played_bucket", 1, 0).over(w_all_by_played_at).alias("prev_ms_played_bucket"),
        F.count(F.lit(1)).over(w_listener_id_listener_plan).alias("rows_in_listener_id_listener_plan"),
        F.lead("played_at").over(w_all_by_played_at).alias("next_played_at"),
        F.nth_value("listener_signup_date", 2).over(w_device_by_ms_played_bucket_f).alias("second_listener_signup_date_2"),
        F.max("ms_played_bucket").over(w_listener_id_by_listener_sign_3).alias("ms_played_bucket_max_4"),
        F.ntile(4).over(w_all_by_played_at).alias("played_at_quartile"),
        F.rank().over(w_all_by_played_at).alias("played_at_rank"),
        F.max("ms_played_bucket").over(w_listener_signup_date_year).alias("listener_signup_date_year_max_ms_played_bucket"),
        F.row_number().over(w_all_by_played_at).alias("rn"),
    )
)

result = (
    windowed
    .select(
        "id",
        "listener_id",
        "played_at",
        "device",
        "ms_played_bucket",
        "listener_plan",
        "listener_signup_date",
        "listener_username",
        "second_listener_signup_date",
        "rows_in_listener_id_listener_plan",
        "second_listener_signup_date_2",
        "ms_played_bucket_max_4",
        "played_at_rank",
        "listener_signup_date_year_max_ms_played_bucket",
        "rn",
    )
    .orderBy(
        F.col("listener_username").asc(),
        F.col("listener_plan").desc_nulls_first(),
    )
)
