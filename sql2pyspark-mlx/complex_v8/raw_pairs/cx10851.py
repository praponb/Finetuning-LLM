from pyspark.sql import functions as F

joined_ranges = (
    df_meter_readings.alias("a")
    .join(
        df_meter_readings.alias("b"),
        (F.col("b.site_id") == F.col("a.site_id"))
        & (F.col("b.read_at") < F.col("a.read_at"))
        & (F.col("b.read_at") >= F.col("a.read_at") - F.expr("INTERVAL 7 DAYS")),
        "left",
    )
    .groupBy("a.id", "a.site_id", "a.read_at")
    .agg(
        F.count("b.id").alias("prior_rows_7d"),
        F.sum("b.kwh").alias("prior_kwh_7d"),
    )
)

site_id_stats = (
    joined_ranges
    .groupBy("site_id")
    .agg(F.max("prior_kwh_7d").alias("max_prior_kwh_7d"))
)

above_average = (
    joined_ranges.alias("s")
    .join(site_id_stats.alias("g"), F.col("g.site_id") == F.col("s.site_id"), "inner")
    .filter(F.col("s.prior_kwh_7d") == F.col("g.max_prior_kwh_7d"))
    .select(
        "s.*",
        "g.max_prior_kwh_7d",
        F.round(F.col("s.prior_kwh_7d") - F.col("g.max_prior_kwh_7d"), 2).alias("prior_kwh_7d_minus_max"),
    )
)

filtered = (
    above_average.alias("s")
    .join(df_meter_readings.alias("m"), (F.col("m.site_id") == F.col("s.site_id")) & (F.col("m.voltage") <= 237.8) & (F.month(F.col("m.read_at")).isin(2, 3)), "left_semi")
)

site_id_stats_2 = (
    filtered
    .groupBy("site_id")
    .agg(F.percentile_approx("prior_rows_7d", 0.5).alias("median_prior_rows_7d"))
)

above_average_2 = (
    filtered.alias("s")
    .join(site_id_stats_2.alias("g"), F.col("g.site_id") == F.col("s.site_id"), "inner")
    .filter(F.col("s.prior_rows_7d") >= F.col("g.median_prior_rows_7d"))
    .select(
        "s.*",
        "g.median_prior_rows_7d",
        F.round(F.col("s.prior_rows_7d") - F.col("g.median_prior_rows_7d"), 2).alias("prior_rows_7d_minus_median"),
    )
)

result = (
    above_average_2
    .select(
        "id",
        "max_prior_kwh_7d",
        "prior_rows_7d_minus_median",
        "median_prior_rows_7d",
    )
    .unpivot(
        ids=["id"],
        values=["max_prior_kwh_7d", "prior_rows_7d_minus_median", "median_prior_rows_7d"],
        variableColumnName="measure",
        valueColumnName="reading",
    )
    .filter(F.col("reading").isNotNull())
    .orderBy("id", "measure")
)
