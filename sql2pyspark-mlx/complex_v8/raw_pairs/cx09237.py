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

rollup_base = (
    joined_ranges
    .groupBy(F.to_date("read_at").alias("read_at_day"), "site_id", "prior_rows_1d")
    .agg(
        F.round(F.avg("prior_voltage_1d"), 2).alias("avg_prior_voltage_1d"),
        F.percentile_approx("prior_voltage_1d", 0.5).alias("median_prior_voltage_1d"),
        F.avg("prior_voltage_1d").alias("avg_prior_voltage_1d_check"),
    )
    .filter(F.col("avg_prior_voltage_1d_check") > 245.0)
    .drop("avg_prior_voltage_1d_check")
)

detail_rows = (
    rollup_base.alias("s")
    .join(df_meter_readings.alias("m"), F.col("m.site_id") == F.col("s.site_id"), "inner")
    .select(
        "s.read_at_day",
        "s.site_id",
        "s.prior_rows_1d",
        F.col("m.id").alias("reading_id"),
        F.col("m.read_at").alias("reading_read_at"),
        F.col("m.voltage").alias("reading_voltage"),
        F.col("m.kwh").alias("reading_kwh"),
    )
)

result = (
    detail_rows
    .select(
        "read_at_day",
        "site_id",
        "prior_rows_1d",
        "reading_id",
        "reading_read_at",
    )
    .orderBy("site_id", "reading_id")
)
