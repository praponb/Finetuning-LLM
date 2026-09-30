from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_all_by_last_played_at_prior = Window.orderBy(F.col("last_played_at").desc(), "id").rowsBetween(-3, -1)
w_all_by_last_played_at_full = Window.orderBy(F.col("last_played_at").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)

prepared_listeners = (
    df_listeners
    .filter((F.col("birth_year") > 1969) & (F.col("country").isNotNull()))
    .select(
        "id",
        "plan",
        "country",
        "signup_date",
        "interests",
        F.datediff(F.lit("2025-06-30").cast("date"), F.col("signup_date")).alias("days_since_signup_date"),
        F.length(F.trim(F.col("username"))).alias("username_len"),
        F.datediff(F.lit("2025-06-30").cast("date"), F.col("signup_date")).alias("days_since_signup_date_2"),
    )
)

play_stats = (
    df_plays
    .groupBy("listener_id")
    .agg(F.round(F.avg("ms_played"), 2).alias("avg_play_ms_played"))
)

summarized_rows = (
    prepared_listeners.alias("s")
    .join(play_stats.alias("p"), F.col("p.listener_id") == F.col("s.id"), "left")
    .select("s.*", "p.avg_play_ms_played")
)

play_stats_2 = (
    df_plays
    .groupBy("listener_id")
    .agg(
        F.min("played_at").alias("first_played_at"),
        F.max("played_at").alias("last_played_at"),
    )
)

profiled = (
    summarized_rows.alias("s")
    .join(play_stats_2.alias("p"), F.col("p.listener_id") == F.col("s.id"), "left")
    .select("s.*", "p.first_played_at", "p.last_played_at")
)

sequenced = (
    profiled
    .withColumn("days_since_signup_date_sum_3_prior", F.sum("days_since_signup_date").over(w_all_by_last_played_at_prior))
    .withColumn("second_avg_play_ms_played", F.nth_value("avg_play_ms_played", 2).over(w_all_by_last_played_at_full))
)

result = (
    sequenced
    .select(
        "id",
        "days_since_signup_date",
        "days_since_signup_date_sum_3_prior",
        "days_since_signup_date_2",
        "username_len",
    )
    .unpivot(
        ids=["id"],
        values=["days_since_signup_date", "days_since_signup_date_sum_3_prior", "days_since_signup_date_2", "username_len"],
        variableColumnName="field",
        valueColumnName="value",
    )
)
