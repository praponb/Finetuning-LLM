from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_tariff = Window.partitionBy("tariff")
w_tariff_by_minutes_valid_to_y = Window.partitionBy("tariff").orderBy("minutes_valid_to_year_to_valid_to", "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)

recent = (
    df_tariff_windows
    .filter((F.col("tariff") != "FLAT") | (F.month(F.col("valid_from")).isin(2, 3)))
    .select(
        "id",
        "tariff",
        "valid_to",
        "rate",
        F.date_trunc("year", F.col("valid_to")).alias("valid_to_year"),
        F.coalesce(F.col("tariff"), F.lit("UNKNOWN")).alias("tariff_or_unknown"),
    )
)

scored = (
    recent
    .filter(F.col("tariff_or_unknown").isin("UNKNOWN", "EV"))
    .select(
        "id",
        "tariff",
        "rate",
        "valid_to_year",
        F.concat(F.col("tariff_or_unknown"), F.lit("#"), F.col("id").cast("string")).alias("tariff_or_unknown_ref"),
        F.round((F.unix_timestamp(F.col("valid_to")) - F.unix_timestamp(F.col("valid_to_year"))) / 60, 1).alias("minutes_valid_to_year_to_valid_to"),
        F.coalesce(F.col("rate"), F.lit(0.0)).alias("rate_filled"),
    )
)

ranked = (
    scored
    .withColumn("last_rate", F.last("rate").over(w_tariff_by_minutes_valid_to_y))
    .withColumn("rate_filled_share_of_tariff", F.round(F.col("rate_filled") / F.nullif(F.sum("rate_filled").over(w_tariff), F.lit(0)), 4))
)

screened = (
    ranked
    .crossJoin(ranked.agg(F.avg("minutes_valid_to_year_to_valid_to").alias("overall_avg_minutes_valid_to_year_to_valid_to")))
    .filter(F.col("minutes_valid_to_year_to_valid_to") > F.col("overall_avg_minutes_valid_to_year_to_valid_to"))
    .drop("overall_avg_minutes_valid_to_year_to_valid_to")
)

result = (
    screened
    .select(
        "id",
        "tariff",
        "rate",
        "valid_to_year",
        "tariff_or_unknown_ref",
        "rate_filled",
        "last_rate",
        "rate_filled_share_of_tariff",
        (F.col("last_rate") + F.col("rate")).alias("last_rate_plus_rate"),
        F.date_trunc("quarter", F.col("valid_to_year")).alias("valid_to_year_quarter"),
    )
    .orderBy(F.col("rate_filled").desc(), F.col("valid_to_year_quarter").desc(), "id")
    .limit(10)
)
