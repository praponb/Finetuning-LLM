from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_tariff_by_valid_to_prior_3 = Window.partitionBy("tariff").orderBy("valid_to", "id").rowsBetween(-3, -1)
w_tariff_by_valid_to_full = Window.partitionBy("tariff").orderBy("valid_to", "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_valid_to_month_by_days_since = Window.partitionBy(F.date_trunc("month", "valid_to")).orderBy("days_since_valid_to", "id")
w_tariff = Window.partitionBy("tariff")

base = (
    df_tariff_windows
    .filter((F.col("valid_to") >= F.lit("2025-02-15 04:00:00").cast("timestamp")) & (F.month(F.col("valid_from")).isin(1, 3)))
    .select(
        "id",
        "tariff",
        "valid_from",
        "valid_to",
        F.datediff(F.lit("2026-01-01").cast("date"), F.col("valid_to")).alias("days_since_valid_to"),
        F.datediff(F.lit("2026-01-01").cast("date"), F.col("valid_from")).alias("days_since_valid_from"),
    )
)

tariff_stats = (
    base
    .groupBy("tariff")
    .agg(F.max("days_since_valid_to").alias("max_days_since_valid_to"))
)

outliers = (
    base.alias("s")
    .join(tariff_stats.alias("g"), F.col("g.tariff") == F.col("s.tariff"), "inner")
    .filter(F.col("s.days_since_valid_to") == F.col("g.max_days_since_valid_to"))
    .select(
        "s.*",
        "g.max_days_since_valid_to",
        F.round(F.col("s.days_since_valid_to") - F.col("g.max_days_since_valid_to"), 2).alias("days_since_valid_to_minus_max"),
    )
)

windowed = (
    outliers
    .select(
        "*",
        F.round(F.avg("days_since_valid_to").over(w_tariff_by_valid_to_prior_3), 2).alias("days_since_valid_to_avg_3_prior"),
        F.last("max_days_since_valid_to").over(w_tariff_by_valid_to_full).alias("last_max_days_since_valid_to"),
        F.lag("valid_from").over(w_valid_to_month_by_days_since).alias("prev_valid_from"),
        F.count(F.lit(1)).over(w_tariff).alias("rows_in_tariff"),
        F.round(F.col("days_since_valid_from") / F.nullif(F.sum("days_since_valid_from").over(w_tariff), F.lit(0)), 4).alias("days_since_valid_from_share_of_tariff"),
    )
)

result = (
    windowed
    .filter(F.col("days_since_valid_to_minus_max") < 420.0)
    .select(
        "id",
        "tariff",
        "valid_from",
        "valid_to",
        "days_since_valid_to",
        "max_days_since_valid_to",
        "days_since_valid_to_minus_max",
        "last_max_days_since_valid_to",
        "prev_valid_from",
        "rows_in_tariff",
    )
    .orderBy(F.col("prev_valid_from").desc())
)
