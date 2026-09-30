from pyspark.sql import functions as F

valid = (
    df_tariff_windows
    .filter(F.col("rate") >= 3.0)
    .select(
        "id",
        "tariff",
        "valid_to",
        "rate",
        F.when(F.col("rate") >= 5.9, "large").when(F.col("rate") >= 4.2, "medium").otherwise("small").alias("rate_tier"),
    )
)

screened = (
    valid
    .crossJoin(valid.agg(F.max("rate").alias("overall_max_rate")))
    .filter(F.col("rate") < F.col("overall_max_rate"))
    .drop("overall_max_rate")
)

qualified = (
    screened
    .crossJoin(screened.agg(F.max("rate").alias("overall_max_rate")))
    .filter(F.col("rate") < F.col("overall_max_rate"))
    .drop("overall_max_rate")
)

per_group = (
    qualified
    .groupBy(F.year("valid_to").alias("valid_to_year"), "rate_tier", "tariff")
    .agg(
        F.min("rate").alias("min_rate"),
        F.sum(F.when(F.col("tariff").isin("FLAT", "EV"), F.col("rate"))).alias("rate_tariff_in_flat_ev"),
        F.sum(F.when(~F.col("tariff").isin("EV", "FLAT"), F.col("rate"))).alias("rate_tariff_not_in_ev_flat"),
        F.count(F.when(F.col("tariff").isin("TOU", "EV"), 1)).alias("n_tariff_in_tou_ev"),
        F.max("valid_to").alias("last_valid_to"),
        F.array_sort(F.collect_set("tariff")).alias("tariff_list"),
        F.sum("rate").alias("sum_rate_check"),
    )
    .filter(F.col("sum_rate_check") > 3.7)
    .drop("sum_rate_check")
)

filtered = (
    per_group
    .crossJoin(per_group.agg(F.avg("valid_to_year").alias("overall_avg_valid_to_year")))
    .filter(F.col("valid_to_year") > F.col("overall_avg_valid_to_year"))
    .drop("overall_avg_valid_to_year")
)

enriched = (
    filtered
    .select(
        "*",
        F.lower(F.trim(F.col("tariff"))).alias("tariff_clean"),
        F.concat_ws(" / ", F.col("rate_tier"), F.col("tariff")).alias("rate_tier_tariff_label"),
    )
)

annotated = (
    enriched
    .select(
        "valid_to_year",
        "rate_tier",
        "tariff",
        "rate_tariff_in_flat_ev",
        "n_tariff_in_tou_ev",
        "last_valid_to",
        F.date_trunc("day", F.col("last_valid_to")).alias("last_valid_to_day"),
        F.datediff(F.lit("2026-01-01").cast("date"), F.col("last_valid_to")).alias("days_since_last_valid_to"),
    )
)

result = (
    annotated
    .groupBy("last_valid_to")
    .pivot("tariff", ["EV", "TOU", "FLAT"])
    .agg(F.max("n_tariff_in_tou_ev"))
)
