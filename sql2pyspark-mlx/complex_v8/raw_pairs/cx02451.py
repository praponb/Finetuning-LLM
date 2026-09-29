from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_latest = Window.partitionBy("tariff").orderBy(F.col("days_since_valid_to").desc(), F.col("id").desc())
w_tariff_by_days_since_valid_t = Window.partitionBy("tariff").orderBy(F.col("days_since_valid_to_filled").desc_nulls_first(), "id")
w_tariff = Window.partitionBy("tariff")
w_tariff_by_days_since_valid_t_2 = Window.partitionBy("tariff").orderBy(F.col("days_since_valid_to_filled").desc_nulls_first(), "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)

scoped_tariffs = (
    df_tariff_windows
    .filter((F.col("tariff") == "FLAT") | (F.to_date(F.col("valid_from")) < F.lit("2025-03-09").cast("date")))
    .select(
        "id",
        "tariff",
        "valid_from",
        "rate",
        F.datediff(F.lit("2025-06-30").cast("date"), F.col("valid_to")).alias("days_since_valid_to"),
    )
)

latest = (
    scoped_tariffs
    .withColumn("rn", F.row_number().over(w_latest))
    .filter(F.col("rn") == 1)
    .drop("rn")
)

enriched = (
    latest
    .filter(F.col("rate") >= 4.2)
    .select(
        "id",
        "tariff",
        "rate",
        "days_since_valid_to",
        F.abs(F.col("rate") - 5.2).alias("rate_distance"),
    )
)

scored = (
    enriched
    .filter(F.col("days_since_valid_to") >= 280)
    .select(
        "id",
        "tariff",
        "rate",
        "days_since_valid_to",
        "rate_distance",
        F.coalesce(F.col("days_since_valid_to"), F.lit(0)).alias("days_since_valid_to_filled"),
        F.abs(F.col("days_since_valid_to") - 385).alias("days_since_valid_to_distance"),
        F.lower(F.trim(F.col("tariff"))).alias("tariff_clean"),
    )
)

ranked = (
    scored
    .select(
        "id",
        "tariff",
        "days_since_valid_to_filled",
        "days_since_valid_to_distance",
        F.round(F.cume_dist().over(w_tariff_by_days_since_valid_t), 3).alias("days_since_valid_to_filled_cume_dist"),
        F.round(F.col("rate") - F.avg("rate").over(w_tariff), 2).alias("rate_vs_tariff_avg"),
        F.last("days_since_valid_to").over(w_tariff_by_days_since_valid_t_2).alias("last_days_since_valid_to"),
    )
)

result = (
    ranked
    .select(
        "id",
        "tariff",
        "days_since_valid_to_filled",
        "days_since_valid_to_filled_cume_dist",
        "rate_vs_tariff_avg",
        "last_days_since_valid_to",
    )
    .orderBy("id", F.col("days_since_valid_to_filled_cume_dist").asc())
    .limit(5)
)
