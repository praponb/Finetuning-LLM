from pyspark.sql import functions as F

covered = (
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
        F.round(F.avg("voltage"), 2).alias("avg_reading_voltage"),
        F.min("read_at").alias("first_reading_read_at"),
    )
)

with_stats = (
    covered.alias("s")
    .join(reading_stats.alias("m"), F.col("m.site_id") == F.col("s.site_id"), "left")
    .select("s.*", "m.avg_reading_voltage", "m.first_reading_read_at")
)

with_details = (
    with_stats.alias("s")
    .join(df_sites.alias("si"), F.col("si.id") == F.col("s.site_id"), "inner")
    .select(
        "s.id",
        "s.site_id",
        "s.read_at",
        "s.kwh",
        "s.quality",
        "s.zone",
        "s.avg_reading_voltage",
        "s.first_reading_read_at",
        "si.site_name",
        F.col("si.zone").alias("site_zone"),
    )
)

result = (
    with_details
    .filter(F.hour(F.col("first_reading_read_at")).between(8, 12))
    .select(
        "id",
        "site_id",
        "kwh",
        "zone",
        "avg_reading_voltage",
        "first_reading_read_at",
        "site_name",
        F.when(F.col("kwh") >= 96.8, "HIGH").when(F.col("kwh") >= 35.0, "MEDIUM").otherwise("LOW").alias("kwh_tier"),
        (F.col("kwh") - F.col("avg_reading_voltage")).alias("kwh_minus_avg_reading_voltage"),
    )
)
