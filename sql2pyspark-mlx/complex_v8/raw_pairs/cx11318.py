from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_tariff_by_rate_ties = Window.partitionBy("tariff").orderBy(F.col("rate").desc())
w_tariff = Window.partitionBy("tariff")
w_tariff_by_rate = Window.partitionBy("tariff").orderBy(F.col("rate").desc(), "id")
w_rate_decile = Window.partitionBy("rate_decile")
w_rate_rank_by_rate_rank_ties = Window.partitionBy("rate_rank").orderBy(F.col("rate_rank").desc())
w_tariff_rate_rank_by_rate_per = Window.partitionBy("tariff", "rate_rank").orderBy(F.col("rate_percent_rank").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_valid_from_month_by_rate_dec = Window.partitionBy(F.month("valid_from")).orderBy(F.col("rate_decile").asc_nulls_last(), "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_tariff_rate_rank_by_prev3_va = Window.partitionBy("tariff", "rate_rank").orderBy("prev3_valid_from", "id").rowsBetween(-3, Window.currentRow)

valid_tariffs = (
    df_tariff_windows
    .filter((F.col("valid_from") >= F.lit("2025-02-15 04:00:00").cast("timestamp")) | (~F.col("tariff").isin("TOU", "EV")))
    .select(
        "id",
        "tariff",
        "valid_from",
        "valid_to",
        "rate",
        F.lower(F.regexp_replace(F.trim(F.col("tariff")), " ", "_")).alias("tariff_slug"),
    )
)

ranked = (
    valid_tariffs
    .select(
        "id",
        "tariff",
        "valid_from",
        "rate",
        F.round(F.percent_rank().over(w_tariff_by_rate_ties), 3).alias("rate_percent_rank"),
        F.lag("valid_from", 3).over(w_tariff_by_rate).alias("prev3_valid_from"),
        F.round(F.col("rate") - F.avg("rate").over(w_tariff), 2).alias("rate_vs_tariff_avg"),
        F.ntile(10).over(w_tariff_by_rate).alias("rate_decile"),
        F.rank().over(w_tariff_by_rate_ties).alias("rate_rank"),
    )
)

with_windows = (
    ranked
    .select(
        "id",
        "tariff",
        "valid_from",
        "rate",
        "rate_percent_rank",
        "prev3_valid_from",
        "rate_vs_tariff_avg",
        "rate_decile",
        "rate_rank",
        F.round(F.col("rate_rank") / F.nullif(F.sum("rate_rank").over(w_rate_decile), F.lit(0)), 4).alias("rate_rank_share_of_rate_decile"),
        F.round(F.percent_rank().over(w_rate_rank_by_rate_rank_ties), 3).alias("rate_rank_percent_rank"),
    )
)

windowed = (
    with_windows
    .select(
        "id",
        "tariff",
        "valid_from",
        "rate",
        "rate_percent_rank",
        "prev3_valid_from",
        "rate_vs_tariff_avg",
        "rate_rank",
        F.last("rate_percent_rank").over(w_tariff_rate_rank_by_rate_per).alias("last_rate_percent_rank"),
        F.last("rate_vs_tariff_avg").over(w_valid_from_month_by_rate_dec).alias("last_rate_vs_tariff_avg"),
    )
)

enriched = (
    windowed
    .select(
        "id",
        "tariff",
        "rate",
        "rate_percent_rank",
        "prev3_valid_from",
        "rate_vs_tariff_avg",
        "rate_rank",
        F.greatest(F.col("rate_vs_tariff_avg"), F.col("last_rate_percent_rank")).alias("greatest_rate_vs_tariff_avg_last_rate_percent_ra"),
    )
)

tagged = (
    enriched
    .select(
        "*",
        F.greatest(F.col("rate_percent_rank"), F.col("rate_rank")).alias("greatest_rate_percent_rank_rate_rank"),
        F.datediff(F.lit("2025-06-30").cast("date"), F.col("prev3_valid_from")).alias("days_since_prev3_valid_from"),
    )
)

sequenced = (
    tagged
    .select(
        "id",
        "tariff",
        "prev3_valid_from",
        "rate_rank",
        "greatest_rate_vs_tariff_avg_last_rate_percent_ra",
        "days_since_prev3_valid_from",
        F.max("rate_percent_rank").over(w_tariff_rate_rank_by_prev3_va).alias("rate_percent_rank_max_4"),
        F.round(F.col("rate_percent_rank") / F.nullif(F.sum("rate_percent_rank").over(w_tariff), F.lit(0)), 4).alias("rate_percent_rank_share_of_tariff"),
    )
)

result = (
    sequenced
    .filter(F.col("rate_rank") <= 6)
    .select(
        "id",
        "tariff",
        "prev3_valid_from",
        "rate_rank",
        "greatest_rate_vs_tariff_avg_last_rate_percent_ra",
        "rate_percent_rank_max_4",
        "rate_percent_rank_share_of_tariff",
        (F.col("rate_rank") + F.col("rate_percent_rank_max_4")).alias("rate_rank_plus_rate_percent_rank_max_4"),
    )
    .orderBy("rate_percent_rank_max_4")
)
