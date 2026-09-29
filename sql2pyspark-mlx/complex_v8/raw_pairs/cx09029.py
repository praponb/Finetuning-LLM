from pyspark.sql import functions as F

matched = (
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

line_level = (
    matched.alias("s")
    .join(df_meter_readings.alias("m"), F.col("m.site_id") == F.col("s.site_id"), "inner")
    .select(
        "s.id",
        "s.site_id",
        "s.kwh",
        "s.voltage",
        "s.energy_cost",
        F.col("m.id").alias("reading_id"),
        F.col("m.voltage").alias("reading_voltage"),
        F.col("m.quality").alias("reading_quality"),
        F.col("m.kwh").alias("reading_kwh"),
        F.col("m.read_at").alias("reading_read_at"),
    )
)

with_details = (
    line_level.alias("s")
    .join(df_meter_readings.alias("m"), F.col("m.id") == F.col("s.reading_id"), "left")
    .select(
        "s.id",
        "s.site_id",
        "s.kwh",
        "s.voltage",
        "s.energy_cost",
        "s.reading_id",
        "s.reading_voltage",
        "s.reading_quality",
        "s.reading_kwh",
        "s.reading_read_at",
        F.col("m.kwh").alias("reading_kwh_2"),
    )
)

result = (
    with_details
    .cube("reading_quality", "site_id", "reading_id")
    .agg(
        F.grouping("reading_quality").alias("is_total_reading_quality"),
        F.countDistinct("reading_quality").alias("distinct_reading_qualitys"),
        F.max("energy_cost").alias("max_energy_cost"),
    )
    .select(
        F.coalesce(F.col("reading_quality"), F.lit("ALL")).alias("reading_quality"),
        "site_id",
        "reading_id",
        "is_total_reading_quality",
        "distinct_reading_qualitys",
        "max_energy_cost",
    )
)
