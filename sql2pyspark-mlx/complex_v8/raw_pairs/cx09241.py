from pyspark.sql import functions as F

joined_ranges = (
    df_meter_readings.alias("a")
    .join(
        df_meter_readings.alias("b"),
        (F.col("b.site_id") == F.col("a.site_id"))
        & (F.col("b.read_at") < F.col("a.read_at"))
        & (F.col("b.read_at") >= F.col("a.read_at") - F.expr("INTERVAL 1 DAYS")),
        "left",
    )
    .groupBy("a.id", "a.site_id", "a.read_at")
    .agg(
        F.count("b.id").alias("prior_rows_1d"),
        F.sum("b.voltage").alias("prior_voltage_1d"),
    )
)

site_id_stats = (
    joined_ranges
    .groupBy("site_id")
    .agg(F.avg("prior_rows_1d").alias("avg_prior_rows_1d"))
)

above_average = (
    joined_ranges.alias("s")
    .join(site_id_stats.alias("g"), F.col("g.site_id") == F.col("s.site_id"), "inner")
    .filter(F.col("s.prior_rows_1d") > F.col("g.avg_prior_rows_1d"))
    .select(
        "s.*",
        "g.avg_prior_rows_1d",
        F.round(F.col("s.prior_rows_1d") - F.col("g.avg_prior_rows_1d"), 2).alias("prior_rows_1d_minus_avg"),
    )
)

result = (
    above_average
    .select(
        "id",
        "site_id",
        "read_at",
        "prior_rows_1d",
        "prior_voltage_1d",
        F.to_date(F.col("read_at")).alias("read_at_date"),
        F.abs(F.col("prior_voltage_1d") - 367.5).alias("prior_voltage_1d_distance"),
    )
)
