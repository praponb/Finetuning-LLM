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
        "r.voltage",
        "st.zone",
        "tw.rate",
        F.round(F.col("r.kwh") * F.col("tw.rate"), 2).alias("energy_cost"),
    )
)

filtered = (
    joined_ranges.alias("s")
    .join(df_sites.alias("si"), (F.col("si.id") == F.col("s.site_id")) & (F.col("si.installed_kw").between(93.3, 446.9)) & (F.length(F.trim(F.col("si.site_name"))) >= 7), "left_semi")
)

detail_rows_readings = (
    filtered.alias("s")
    .join(df_meter_readings.alias("m"), F.col("m.site_id") == F.col("s.site_id"), "inner")
    .select(
        "s.id",
        "s.site_id",
        "s.read_at",
        "s.zone",
        "s.energy_cost",
        F.col("m.id").alias("reading_id"),
        F.col("m.quality").alias("reading_quality"),
        F.col("m.kwh").alias("reading_kwh"),
    )
)

reading_stats = (
    df_meter_readings
    .groupBy("site_id")
    .agg(
        F.count("*").alias("reading_count"),
        F.max("read_at").alias("last_reading_read_at"),
        F.min("read_at").alias("first_reading_read_at"),
    )
)

summarized_rows = (
    detail_rows_readings.alias("s")
    .join(reading_stats.alias("m"), F.col("m.site_id") == F.col("s.site_id"), "left")
    .select(
        "s.*",
        F.coalesce(F.col("m.reading_count"), F.lit(0)).alias("reading_count"),
        "m.last_reading_read_at",
        "m.first_reading_read_at",
    )
)

result = (
    summarized_rows
    .select(
        "id",
        "site_id",
        "read_at",
        "zone",
        "energy_cost",
        "reading_quality",
        "reading_kwh",
        "last_reading_read_at",
        F.round((F.unix_timestamp(F.col("first_reading_read_at")) - F.unix_timestamp(F.col("read_at"))) / 3600, 1).alias("hours_read_at_to_first_reading_read_at"),
        F.when(F.col("energy_cost") >= 585.0, "HIGH").when(F.col("energy_cost") >= 225.0, "MEDIUM").otherwise("LOW").alias("energy_cost_tier"),
    )
    .orderBy(F.col("id").asc())
)
