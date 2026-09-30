from pyspark.sql import functions as F

matched = (
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

with_parent = (
    matched.alias("s")
    .join(df_sites.alias("si"), F.col("si.id") == F.col("s.site_id"), "inner")
    .select(
        "s.id",
        "s.site_id",
        "s.read_at",
        "s.prior_rows_12h",
        "s.prior_voltage_12h",
        "si.site_name",
    )
)

site_id_stats = (
    with_parent
    .groupBy("site_id")
    .agg(F.avg("prior_rows_12h").alias("avg_prior_rows_12h"))
)

above_average = (
    with_parent.alias("s")
    .join(site_id_stats.alias("g"), F.col("g.site_id") == F.col("s.site_id"), "inner")
    .filter(F.col("s.prior_rows_12h") > F.col("g.avg_prior_rows_12h"))
    .select(
        "s.*",
        "g.avg_prior_rows_12h",
        F.round(F.col("s.prior_rows_12h") - F.col("g.avg_prior_rows_12h"), 2).alias("prior_rows_12h_minus_avg"),
    )
)

with_activity = (
    above_average.alias("s")
    .join(df_meter_readings.alias("m"), F.col("m.site_id") == F.col("s.site_id"), "left_semi")
)

result = (
    with_activity
    .select(
        "id",
        "site_id",
        "read_at",
        "prior_rows_12h",
        "prior_voltage_12h",
        "avg_prior_rows_12h",
        "prior_rows_12h_minus_avg",
        F.upper(F.trim(F.col("site_name"))).alias("site_name_clean"),
        F.date_trunc("month", F.col("read_at")).alias("read_at_month"),
    )
    .orderBy(F.col("site_id").desc(), "id")
    .limit(10)
)
