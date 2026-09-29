from pyspark.sql import functions as F

base = (
    df_tariff_windows
    .filter(F.col("valid_to") >= F.lit("2025-02-16 05:00:00").cast("timestamp"))
    .select(
        "id",
        "tariff",
        "valid_from",
        "rate",
        F.round((F.unix_timestamp(F.col("valid_from")) - F.unix_timestamp(F.col("valid_to"))) / 3600, 1).alias("hours_valid_to_to_valid_from"),
        F.date_format(F.col("valid_to"), "MMM yyyy").alias("valid_to_label"),
    )
)

tariff_stats = (
    base
    .groupBy("tariff")
    .agg(F.avg("hours_valid_to_to_valid_from").alias("avg_hours_valid_to_to_valid_from"))
)

above_average = (
    base.alias("s")
    .join(tariff_stats.alias("g"), F.col("g.tariff") == F.col("s.tariff"), "inner")
    .filter(F.col("s.hours_valid_to_to_valid_from") > F.col("g.avg_hours_valid_to_to_valid_from"))
    .select(
        "s.*",
        "g.avg_hours_valid_to_to_valid_from",
        F.round(F.col("s.hours_valid_to_to_valid_from") - F.col("g.avg_hours_valid_to_to_valid_from"), 2).alias("hours_valid_to_to_valid_from_minus_avg"),
    )
)

derived = (
    above_average
    .select(
        "id",
        "tariff",
        "valid_from",
        "rate",
        "hours_valid_to_to_valid_from",
        "valid_to_label",
        "hours_valid_to_to_valid_from_minus_avg",
        F.concat_ws(": ", F.col("tariff"), F.col("valid_to_label")).alias("tariff_valid_to_label_label"),
        F.when(F.col("rate") >= 5.9, "HIGH").when(F.col("rate") >= 4.2, "MEDIUM").otherwise("LOW").alias("rate_tier"),
        F.lower(F.regexp_replace(F.trim(F.col("valid_to_label")), " ", "_")).alias("valid_to_label_slug"),
    )
)

without_matches = (
    derived
    .crossJoin(derived.agg(F.avg("rate").alias("overall_avg_rate")))
    .filter(F.col("rate") > F.col("overall_avg_rate"))
    .drop("overall_avg_rate")
)

result = (
    without_matches
    .groupBy("valid_from")
    .pivot("rate_tier", ["HIGH", "MEDIUM"])
    .agg(F.avg("rate"))
    .orderBy("valid_from")
)
