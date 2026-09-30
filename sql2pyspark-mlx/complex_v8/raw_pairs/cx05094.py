from pyspark.sql import functions as F

joined_ranges = (
    df_meter_readings.alias("r")
    .join(df_sites.alias("st"), F.col("st.id") == F.col("r.site_id"), "inner")
    .join(
        df_tariff_windows.alias("tw"),
        (F.col("tw.tariff") == F.col("st.tariff"))
        & (F.col("r.read_at") >= F.col("tw.valid_from"))
        & (F.col("r.read_at") < F.col("tw.valid_to")),
        "left",
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

zone_stats = (
    joined_ranges
    .groupBy("zone")
    .agg(F.max("rate").alias("max_rate"))
)

outliers = (
    joined_ranges.alias("s")
    .join(zone_stats.alias("g"), F.col("g.zone") == F.col("s.zone"), "inner")
    .filter(F.col("s.rate") == F.col("g.max_rate"))
    .select(
        "s.*",
        "g.max_rate",
        F.round(F.col("s.rate") - F.col("g.max_rate"), 2).alias("rate_minus_max"),
    )
)

exploded_detail = (
    outliers.alias("s")
    .join(df_meter_readings.alias("m"), F.col("m.site_id") == F.col("s.site_id"), "left")
    .select(
        "s.id",
        "s.site_id",
        "s.read_at",
        "s.kwh",
        "s.voltage",
        "s.rate",
        "s.energy_cost",
        "s.max_rate",
        F.col("m.id").alias("reading_id"),
        F.col("m.voltage").alias("reading_voltage"),
        F.col("m.read_at").alias("reading_read_at"),
    )
)

with_details_site = (
    exploded_detail.alias("s")
    .join(df_sites.alias("si"), F.col("si.id") == F.col("s.site_id"), "inner")
    .join(df_meter_readings.alias("m"), F.col("m.id") == F.col("s.reading_id"), "inner")
    .filter(F.col("si.zone") == "Z3")
    .select(
        "s.id",
        "s.site_id",
        "s.read_at",
        "s.kwh",
        "s.rate",
        "s.energy_cost",
        "s.max_rate",
        "s.reading_id",
        "s.reading_voltage",
        "s.reading_read_at",
        "si.site_name",
        F.col("si.zone").alias("site_zone"),
        F.col("si.commissioned_on").alias("site_commissioned_on"),
        F.col("m.quality").alias("reading_quality"),
    )
)

result = (
    with_details_site
    .select(
        "id",
        "site_id",
        "read_at",
        "kwh",
        "rate",
        "energy_cost",
        "max_rate",
        "reading_id",
        "reading_voltage",
        "reading_read_at",
        "site_name",
        "site_zone",
        "site_commissioned_on",
        F.lower(F.regexp_replace(F.trim(F.col("site_name")), " ", "_")).alias("site_name_slug"),
        F.round((F.unix_timestamp(F.col("read_at")) - F.unix_timestamp(F.col("reading_read_at"))) / 3600, 1).alias("hours_reading_read_at_to_read_at"),
    )
    .orderBy("kwh", F.col("site_id").desc())
)
