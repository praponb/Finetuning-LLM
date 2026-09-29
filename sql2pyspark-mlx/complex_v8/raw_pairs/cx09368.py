from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_listener_id_prior_rows_12h = Window.partitionBy("listener_id", "prior_rows_12h")
w_listener_id_listener_country = Window.partitionBy("listener_id", "listener_country").orderBy(F.col("prior_ms_played_12h").desc_nulls_first(), "id")

joined_ranges = (
    df_plays.alias("a")
    .join(
        df_plays.alias("b"),
        (F.col("b.listener_id") == F.col("a.listener_id"))
        & (F.col("b.played_at") < F.col("a.played_at"))
        & (F.col("b.played_at") >= F.col("a.played_at") - F.expr("INTERVAL 12 HOURS")),
        "left",
    )
    .groupBy("a.id", "a.listener_id", "a.played_at")
    .agg(
        F.count("b.id").alias("prior_rows_12h"),
        F.sum("b.ms_played").alias("prior_ms_played_12h"),
    )
)

joined_listener = (
    joined_ranges.alias("s")
    .join(df_listeners.alias("l"), F.col("l.id") == F.col("s.listener_id"), "inner")
    .filter(F.col("l.signup_date").between(F.lit("2024-07-13").cast("date"), F.lit("2025-03-18").cast("date")))
    .select(
        "s.id",
        "s.listener_id",
        "s.prior_rows_12h",
        "s.prior_ms_played_12h",
        F.col("l.country").alias("listener_country"),
        F.col("l.signup_date").alias("listener_signup_date"),
        F.col("l.username").alias("listener_username"),
    )
)

ranked = (
    joined_listener
    .select(
        "*",
        F.min("prior_rows_12h").over(w_listener_id_prior_rows_12h).alias("listener_id_prior_rows_12h_min_prior_rows_12h"),
        F.ntile(5).over(w_listener_id_listener_country).alias("prior_ms_played_12h_quintile"),
        F.count(F.lit(1)).over(w_listener_id_prior_rows_12h).alias("rows_in_listener_id_prior_rows_12h"),
    )
)

result = (
    ranked
    .select(
        "id",
        "prior_rows_12h",
        "prior_ms_played_12h",
        "listener_country",
        "listener_username",
        "listener_id_prior_rows_12h_min_prior_rows_12h",
        "prior_ms_played_12h_quintile",
        F.when(F.col("listener_country").isin("UK", "BR"), "group_a").when(F.col("listener_country") == "TH", "group_b").otherwise("other").alias("listener_country_group"),
    )
)
