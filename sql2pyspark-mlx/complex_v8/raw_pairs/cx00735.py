from pyspark.sql import functions as F

filtered_readings = (
    df_meter_readings
    .filter(F.col("site_id") >= 2)
    .select(
        "id",
        "site_id",
        "read_at",
        "kwh",
        "voltage",
        F.round(F.col("voltage") / F.nullif(F.col("kwh"), F.lit(0)), 3).alias("voltage_per_kwh"),
        F.coalesce(F.col("kwh"), F.lit(0.0)).alias("kwh_filled"),
        F.date_trunc("month", F.col("read_at")).alias("read_at_month"),
    )
)

site_id_stats = (
    filtered_readings
    .groupBy("site_id")
    .agg(F.percentile_approx("kwh_filled", 0.5).alias("median_kwh_filled"))
)

above_average = (
    filtered_readings.alias("s")
    .join(site_id_stats.alias("g"), F.col("g.site_id") == F.col("s.site_id"), "inner")
    .filter(F.col("s.kwh_filled") >= F.col("g.median_kwh_filled"))
    .select(
        "s.*",
        "g.median_kwh_filled",
        F.round(F.col("s.kwh_filled") - F.col("g.median_kwh_filled"), 2).alias("kwh_filled_minus_median"),
    )
)

result = (
    above_average
    .select(
        "id",
        F.col("voltage").alias("m_voltage"),
        F.col("kwh_filled").alias("kwh_filled_v"),
        F.col("voltage_per_kwh").alias("voltage_per_kwh_v"),
    )
    .unpivot(
        ids=["id"],
        values=["m_voltage", "kwh_filled_v", "voltage_per_kwh_v"],
        variableColumnName="measure",
        valueColumnName="value",
    )
    .orderBy("id", "measure")
)
