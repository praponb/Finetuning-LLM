from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_played_at_to_date_listener_p = Window.partitionBy(F.to_date("played_at"), "listener_plan").orderBy(F.col("listener_signup_date").desc())
w_all_by_listener_signup_date = Window.orderBy("listener_signup_date", "id")
w_all_by_listener_signup_date_2 = Window.orderBy("listener_signup_date")
w_played_at_date_year_device_b = Window.partitionBy(F.year("played_at_date"), "device").orderBy(F.col("listener_signup_date").desc_nulls_first())
w_all_by_listener_signup_date_3 = Window.orderBy("listener_signup_date", "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_played_at_date_year_device_b_2 = Window.partitionBy(F.year("played_at_date"), "device").orderBy(F.col("listener_signup_date").desc_nulls_first(), "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_played_at_date_year_device_b_3 = Window.partitionBy(F.year("played_at_date"), "device").orderBy(F.col("listener_signup_date").desc_nulls_first(), "id")
w_played_at_year_by_played_at = Window.partitionBy(F.year("played_at")).orderBy("played_at", "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)

valid = (
    df_plays
    .filter((~F.col("source").isin("radio", "album")) & (~F.col("is_skipped")))
    .select(
        "id",
        "listener_id",
        "track_id",
        "played_at",
        "device",
        "is_skipped",
        "source",
        F.to_date(F.col("played_at")).alias("played_at_date"),
    )
)

joined_listener = (
    valid.alias("s")
    .join(df_listeners.alias("l"), F.col("l.id") == F.col("s.listener_id"), "inner")
    .filter(F.col("l.signup_date") >= F.lit("2024-08-02").cast("date"))
    .select(
        "s.id",
        "s.listener_id",
        "s.track_id",
        "s.played_at",
        "s.device",
        "s.source",
        "s.played_at_date",
        F.col("l.plan").alias("listener_plan"),
        F.col("l.signup_date").alias("listener_signup_date"),
    )
)

windowed = (
    joined_listener
    .select(
        "*",
        F.dense_rank().over(w_played_at_to_date_listener_p).alias("listener_signup_date_dense_rank"),
        F.ntile(4).over(w_all_by_listener_signup_date).alias("listener_signup_date_quartile"),
        F.round(F.percent_rank().over(w_all_by_listener_signup_date_2), 3).alias("listener_signup_date_percent_rank"),
        F.rank().over(w_played_at_date_year_device_b).alias("listener_signup_date_rank"),
        F.rank().over(w_played_at_to_date_listener_p).alias("listener_signup_date_rank_2"),
        F.nth_value("listener_signup_date", 2).over(w_all_by_listener_signup_date_3).alias("second_listener_signup_date"),
        F.rank().over(w_all_by_listener_signup_date_2).alias("listener_signup_date_rank_3"),
        F.dense_rank().over(w_all_by_listener_signup_date_2).alias("listener_signup_date_dense_rank_2"),
        F.first("played_at", ignorenulls=True).over(w_played_at_date_year_device_b_2).alias("first_known_played_at"),
        F.dense_rank().over(w_played_at_date_year_device_b_3).alias("listener_signup_date_dense_rank_3"),
        F.rank().over(w_all_by_listener_signup_date).alias("listener_signup_date_rank_5"),
        F.nth_value("played_at", 2).over(w_played_at_year_by_played_at).alias("second_played_at"),
        F.last("played_at_date").over(w_all_by_listener_signup_date_3).alias("last_played_at_date"),
    )
)

result = (
    windowed
    .filter(F.col("last_played_at_date").isNotNull())
    .select(
        "id",
        "listener_id",
        "played_at",
        "listener_plan",
        "listener_signup_date",
        "listener_signup_date_dense_rank",
        "listener_signup_date_percent_rank",
        "listener_signup_date_rank",
        "listener_signup_date_rank_2",
        "second_listener_signup_date",
        "listener_signup_date_dense_rank_2",
        "first_known_played_at",
        "listener_signup_date_dense_rank_3",
        "second_played_at",
    )
    .orderBy(F.col("listener_signup_date_dense_rank").desc_nulls_first())
)
