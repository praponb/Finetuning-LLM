from pyspark.sql import functions as F

in_window = (
    df_meter_readings.alias("a")
    .join(
        df_meter_readings.alias("b"),
        (F.col("b.site_id") == F.col("a.site_id"))
        & (F.col("b.read_at") < F.col("a.read_at"))
        & (F.col("b.read_at") >= F.col("a.read_at") - F.expr("INTERVAL 12 HOURS")),
        "left",
    )
    .groupBy("a.id", "a.site_id", "a.read_at")
    .agg(
        F.count("b.id").alias("prior_rows_12h"),
        F.sum("b.voltage").alias("prior_voltage_12h"),
    )
)

per_group = (
    in_window
    .filter(F.col("prior_voltage_12h") > 306.2)
    .groupBy(
        "prior_rows_12h",
        F.date_trunc("month", "read_at").alias("read_at_month"),
        "site_id",
    )
    .agg(
        F.percentile_approx("prior_voltage_12h", 0.5).alias("median_prior_voltage_12h"),
        F.round(F.avg("prior_rows_12h"), 2).alias("avg_prior_rows_12h"),
    )
)

result = (
    per_group
    .select("prior_rows_12h", "read_at_month", "site_id")
    .orderBy(F.col("read_at_month").desc(), F.col("prior_rows_12h").desc())
)
