from pyspark.sql import functions as F

cleaned_tariffs = (
    df_tariff_windows
    .filter((F.col("valid_to") >= F.lit("2025-02-15 04:00:00").cast("timestamp")) | (F.col("rate") >= 4.2))
    .select(
        "id",
        "tariff",
        "valid_from",
        "valid_to",
        F.date_format(F.col("valid_to"), "yyyy-MM").alias("valid_to_label"),
    )
)

stats = (
    cleaned_tariffs
    .groupBy(
        F.year("valid_from").alias("valid_from_year"),
        F.year("valid_to").alias("valid_to_year"),
        "tariff",
    )
    .agg(
        F.count("*").alias("n_rows"),
        F.min("valid_to").alias("first_valid_to"),
        F.sum(F.when(F.col("tariff").isin("FLAT", "EV"), 1).otherwise(0)).alias("n_tariff_in_flat_ev"),
        F.sum(F.when(F.col("tariff").isNotNull(), 1).otherwise(0)).alias("n_tariff_is_not_null"),
    )
)

enriched = (
    stats
    .select(
        "valid_from_year",
        "valid_to_year",
        "tariff",
        "n_rows",
        "first_valid_to",
        F.month(F.col("first_valid_to")).alias("first_valid_to_month"),
        F.abs(F.col("n_tariff_is_not_null") - 4).alias("n_tariff_is_not_null_distance"),
    )
)

result = (
    enriched
    .groupingSets(
        [["first_valid_to_month", "valid_from_year", "n_rows"], ["first_valid_to_month"], ["valid_from_year"]],
        "first_valid_to_month", "valid_from_year", "n_rows",
    )
    .agg(
        F.grouping("first_valid_to_month").alias("is_total_first_valid_to_month"),
        F.round(F.stddev("n_rows"), 2).alias("stddev_n_rows"),
        F.count(F.when(F.col("tariff").isNotNull(), 1)).alias("n_tariff_is_not_null"),
    )
)
