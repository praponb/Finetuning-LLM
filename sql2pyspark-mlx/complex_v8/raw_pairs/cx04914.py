from pyspark.sql import functions as F

covered = (
    df_meter_readings.alias("a")
    .join(
        df_meter_readings.alias("b"),
        (F.col("b.site_id") == F.col("a.site_id"))
        & (F.col("b.read_at") < F.col("a.read_at"))
        & (F.col("b.read_at") >= F.col("a.read_at") - F.expr("INTERVAL 1 DAYS")),
        "left",
    )
    .groupBy("a.id", "a.site_id", "a.read_at")
    .agg(
        F.count("b.id").alias("prior_rows_1d"),
        F.sum("b.voltage").alias("prior_voltage_1d"),
    )
)

with_details_site = (
    covered.alias("s")
    .join(df_sites.alias("si"), F.col("si.id") == F.col("s.site_id"), "left")
    .select(
        "s.id",
        "s.site_id",
        "s.prior_rows_1d",
        "s.prior_voltage_1d",
        F.col("si.tariff").alias("site_tariff"),
        F.col("si.zone").alias("site_zone"),
    )
)

exploded_detail = (
    with_details_site.alias("s")
    .join(df_meter_readings.alias("m"), F.col("m.site_id") == F.col("s.site_id"), "left")
    .select(
        "s.id",
        "s.site_id",
        "s.prior_rows_1d",
        "s.prior_voltage_1d",
        "s.site_tariff",
        "s.site_zone",
        F.col("m.id").alias("reading_id"),
        F.col("m.kwh").alias("reading_kwh"),
        F.col("m.read_at").alias("reading_read_at"),
    )
)

result = (
    exploded_detail
    .filter(F.col("site_zone") == "Z4")
    .select(
        "id",
        "site_id",
        "prior_rows_1d",
        "prior_voltage_1d",
        "site_zone",
        "reading_id",
        "reading_kwh",
        "reading_read_at",
    )
    .orderBy(F.col("id").desc_nulls_first(), "prior_rows_1d", "reading_id")
    .limit(20)
)
