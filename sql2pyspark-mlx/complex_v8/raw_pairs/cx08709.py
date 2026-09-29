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
        "r.voltage",
        "st.zone",
        "tw.rate",
        F.round(F.col("r.kwh") * F.col("tw.rate"), 2).alias("energy_cost"),
    )
)

detail_rows = (
    covered.alias("s")
    .join(df_meter_readings.alias("m"), (F.col("m.site_id") == F.col("s.site_id")) & (F.to_date(F.col("m.read_at")) < F.lit("2025-02-12").cast("date")), "left")
    .select(
        "s.id",
        "s.site_id",
        "s.read_at",
        "s.kwh",
        "s.voltage",
        "s.zone",
        "s.rate",
        "s.energy_cost",
        F.col("m.id").alias("reading_id"),
        F.col("m.read_at").alias("reading_read_at"),
        F.col("m.voltage").alias("reading_voltage"),
    )
)

screened = (
    detail_rows.alias("s")
    .join(df_meter_readings.alias("m"), (F.col("m.site_id") == F.col("s.site_id")) & (~F.col("m.quality").isin("OK", "EST")), "left_anti")
    .filter(F.col("kwh") < 110.5)
)

result = (
    screened
    .select(
        "id",
        "reading_id",
        "kwh",
        "rate",
        "energy_cost",
    )
    .unpivot(
        ids=["id", "reading_id"],
        values=["kwh", "rate", "energy_cost"],
        variableColumnName="measure",
        valueColumnName="reading",
    )
    .orderBy("id", "reading_id", "measure")
)
