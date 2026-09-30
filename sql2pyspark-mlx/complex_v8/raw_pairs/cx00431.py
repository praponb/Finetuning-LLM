from pyspark.sql import functions as F

joined_ranges = (
    df_meter_readings.alias("r")
    .join(df_sites.alias("st"), F.col("st.id") == F.col("r.site_id"), "inner")
    .join(
        df_tariff_windows.alias("tw"),
        (F.col("tw.tariff") == F.col("st.tariff"))
        & (F.col("r.read_at") >= F.col("tw.valid_from"))
        & (F.col("r.read_at") < F.col("tw.valid_to")),
        "inner",
    )
    .select(
        "r.id",
        "r.site_id",
        "r.read_at",
        "r.kwh",
        "r.quality",
        "st.zone",
        "tw.rate",
        F.round(F.col("r.kwh") * F.col("tw.rate"), 2).alias("energy_cost"),
    )
)

reading_stats = (
    df_meter_readings
    .groupBy("site_id")
    .agg(
        F.count(F.when(F.col("quality") == "OK", 1)).alias("reading_quality_count"),
        F.sum("kwh").alias("total_reading_kwh"),
    )
)

profiled = (
    joined_ranges.alias("s")
    .join(reading_stats.alias("m"), F.col("m.site_id") == F.col("s.site_id"), "left")
    .select(
        "s.*",
        F.coalesce(F.col("m.reading_quality_count"), F.lit(0)).alias("reading_quality_count"),
        "m.total_reading_kwh",
    )
)

reading_stats_2 = (
    df_meter_readings
    .groupBy("site_id")
    .agg(F.min("kwh").alias("min_reading_kwh"))
)

summarized_rows = (
    profiled.alias("s")
    .join(reading_stats_2.alias("m"), F.col("m.site_id") == F.col("s.site_id"), "left")
    .select("s.*", "m.min_reading_kwh")
)

detail_rows_readings = (
    summarized_rows.alias("s")
    .join(df_meter_readings.alias("m"), F.col("m.site_id") == F.col("s.site_id"), "left")
    .select(
        "s.id",
        "s.site_id",
        "s.kwh",
        "s.rate",
        "s.energy_cost",
        "s.total_reading_kwh",
        "s.min_reading_kwh",
        F.col("m.id").alias("reading_id"),
        F.col("m.voltage").alias("reading_voltage"),
        F.col("m.kwh").alias("reading_kwh"),
    )
)

result = (
    detail_rows_readings
    .filter(F.col("site_id") >= 8)
    .select(
        "id",
        "site_id",
        "kwh",
        "rate",
        "energy_cost",
        "total_reading_kwh",
        "min_reading_kwh",
        "reading_id",
        "reading_voltage",
        "reading_kwh",
        F.coalesce(F.col("kwh"), F.lit(0.0)).alias("kwh_filled"),
        (F.col("reading_kwh") - F.col("min_reading_kwh")).alias("reading_kwh_minus_min_reading_kwh"),
    )
)
