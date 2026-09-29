from pyspark.sql import functions as F

covered = (
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

with_children_readings = (
    covered.alias("s")
    .join(df_meter_readings.alias("m"), F.col("m.site_id") == F.col("s.site_id"), "inner")
    .select(
        "s.id",
        "s.site_id",
        "s.read_at",
        "s.kwh",
        "s.quality",
        "s.zone",
        "s.energy_cost",
        F.col("m.id").alias("reading_id"),
        F.col("m.quality").alias("reading_quality"),
        F.col("m.voltage").alias("reading_voltage"),
        F.col("m.read_at").alias("reading_read_at"),
    )
)

result = (
    with_children_readings
    .filter(F.col("quality") == "OK")
    .select(
        "id",
        "site_id",
        "read_at",
        "kwh",
        "quality",
        "zone",
        "reading_id",
        "reading_voltage",
    )
    .orderBy(F.col("kwh").desc())
)
