from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_tariff_by_rate = Window.partitionBy("tariff").orderBy(F.col("rate").desc(), "id")
w_tariff_by_rate_ties = Window.partitionBy("tariff").orderBy(F.col("rate").desc())
w_tariff_by_rate_full = Window.partitionBy("tariff").orderBy(F.col("rate").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_rate_tier_by_rate_percent_ra = Window.partitionBy("rate_tier").orderBy(F.col("rate_percent_rank").asc_nulls_last(), "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_rate_tier = Window.partitionBy("rate_tier")
w_rate_tier_by_rate_percent_ra_2 = Window.partitionBy("rate_tier").orderBy(F.col("rate_percent_rank").asc_nulls_last(), "id")

valid_tariffs = (
    df_tariff_windows
    .filter(F.col("valid_from") < F.lit("2025-03-09 05:00:00").cast("timestamp"))
    .select(
        "id",
        "tariff",
        "valid_to",
        "rate",
        F.when(F.col("rate") >= 7.6, "HIGH").when(F.col("rate") >= 5.2, "MEDIUM").otherwise("LOW").alias("rate_tier"),
        F.round((F.unix_timestamp(F.col("valid_from")) - F.unix_timestamp(F.col("valid_to"))) / 60, 1).alias("minutes_valid_to_to_valid_from"),
        F.substring(F.trim(F.col("tariff")), 1, 3).alias("tariff_prefix"),
    )
)

sequenced = (
    valid_tariffs
    .select(
        "id",
        "rate_tier",
        "minutes_valid_to_to_valid_from",
        F.round((F.unix_timestamp("valid_to") - F.unix_timestamp(F.lag("valid_to").over(w_tariff_by_rate))) / 60, 1).alias("minutes_since_prev_valid_to"),
        F.round(F.percent_rank().over(w_tariff_by_rate_ties), 3).alias("rate_percent_rank"),
        F.nth_value("valid_to", 2).over(w_tariff_by_rate_full).alias("second_valid_to"),
    )
)

enriched = (
    sequenced
    .select(
        "id",
        "rate_tier",
        "minutes_since_prev_valid_to",
        "rate_percent_rank",
        F.datediff(F.lit("2025-12-31").cast("date"), F.col("second_valid_to")).alias("days_since_second_valid_to"),
    )
)

screened = (
    enriched
    .crossJoin(enriched.agg(F.avg("minutes_since_prev_valid_to").alias("overall_avg_minutes_since_prev_valid_to")))
    .filter(F.col("minutes_since_prev_valid_to") > F.col("overall_avg_minutes_since_prev_valid_to"))
    .drop("overall_avg_minutes_since_prev_valid_to")
)

ranked = (
    screened
    .select(
        "id",
        "rate_percent_rank",
        "days_since_second_valid_to",
        F.nth_value("minutes_since_prev_valid_to", 2).over(w_rate_tier_by_rate_percent_ra).alias("second_minutes_since_prev_valid_to"),
        F.count(F.lit(1)).over(w_rate_tier).alias("rows_in_rate_tier"),
        F.row_number().over(w_rate_tier_by_rate_percent_ra_2).alias("seq_no"),
    )
)

result = (
    ranked
    .filter(F.col("seq_no") <= 8)
    .select(
        "id",
        "days_since_second_valid_to",
        "second_minutes_since_prev_valid_to",
        "rows_in_rate_tier",
        "seq_no",
        F.round(F.col("rate_percent_rank") * 0.25, 2).alias("rate_percent_rank_adj"),
        F.greatest(F.col("rows_in_rate_tier"), F.col("seq_no")).alias("greatest_rows_in_rate_tier_seq_no"),
    )
)
