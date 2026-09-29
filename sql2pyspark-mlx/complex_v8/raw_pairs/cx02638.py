from pyspark.sql import functions as F

matched = (
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

detail_rows_readings = (
    matched.alias("s")
    .join(df_meter_readings.alias("m"), F.col("m.site_id") == F.col("s.site_id"), "inner")
    .select(
        "s.id",
        "s.site_id",
        "s.read_at",
        "s.kwh",
        "s.zone",
        "s.rate",
        "s.energy_cost",
        F.col("m.id").alias("reading_id"),
        F.col("m.kwh").alias("reading_kwh"),
        F.col("m.read_at").alias("reading_read_at"),
        F.col("m.quality").alias("reading_quality"),
    )
)

per_group = (
    detail_rows_readings
    .groupBy(
        F.date_trunc("month", "read_at").alias("read_at_month"),
        "reading_quality",
    )
    .agg(
        F.min("reading_kwh").alias("min_reading_kwh"),
        F.count("*").alias("cnt"),
    )
)

result = (
    per_group
    .select(
        "read_at_month",
        "reading_quality",
        "min_reading_kwh",
        F.date_format(F.col("read_at_month"), "yyyy-MM-dd HH:00").alias("read_at_month_label"),
    )
    .orderBy(F.col("min_reading_kwh").desc())
)
