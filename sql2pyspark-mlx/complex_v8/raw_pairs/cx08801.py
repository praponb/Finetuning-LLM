from pyspark.sql import functions as F

scoped = (
    df_meter_readings
    .filter((F.to_date(F.col("read_at")) < F.lit("2025-03-02").cast("date")) & (F.col("quality").isin("OK", "BAD")))
    .select(
        "id",
        "site_id",
        "read_at",
        "kwh",
        "voltage",
    )
)

with_children = (
    scoped.alias("s")
    .join(df_meter_readings.alias("m"), F.col("m.site_id") == F.col("s.site_id"), "inner")
    .select(
        "s.id",
        "s.site_id",
        "s.read_at",
        "s.kwh",
        "s.voltage",
        F.col("m.id").alias("reading_id"),
        F.col("m.kwh").alias("reading_kwh"),
        F.col("m.voltage").alias("reading_voltage"),
    )
)

combined_details = (
    with_children.alias("s")
    .join(df_sites.alias("si"), F.col("si.id") == F.col("s.site_id"), "inner")
    .select(
        "s.id",
        "s.site_id",
        "s.read_at",
        "s.voltage",
        "s.reading_id",
        "s.reading_kwh",
        "s.reading_voltage",
        "si.site_name",
        F.col("si.commissioned_on").alias("site_commissioned_on"),
        F.col("si.zone").alias("site_zone"),
    )
)

enriched = (
    combined_details
    .filter(F.col("site_id").isNotNull())
    .select(
        "id",
        "read_at",
        "reading_id",
        "reading_kwh",
        "reading_voltage",
        "site_name",
        "site_zone",
        F.round(F.col("reading_voltage") / F.nullif(F.col("voltage"), F.lit(0)), 3).alias("reading_voltage_per_voltage"),
    )
)

enriched_reading = (
    enriched.alias("s")
    .join(df_meter_readings.alias("m"), F.col("m.id") == F.col("s.reading_id"), "inner")
    .select(
        "s.*",
        F.col("m.site_id").alias("reading_site_id"),
        F.col("m.kwh").alias("reading_kwh_2"),
    )
)

reading_stats = (
    df_meter_readings
    .groupBy("site_id")
    .agg(F.count("*").alias("reading_count"))
)

summarized_rows = (
    enriched_reading.alias("s")
    .join(reading_stats.alias("m"), F.col("m.site_id") == F.col("s.reading_site_id"), "left")
    .select(
        "s.*",
        F.coalesce(F.col("m.reading_count"), F.lit(0)).alias("reading_count"),
    )
)

result = (
    summarized_rows
    .select(
        "id",
        "read_at",
        "reading_id",
        "reading_kwh",
        "site_name",
        "reading_voltage_per_voltage",
        "reading_site_id",
        "reading_kwh_2",
        "reading_count",
    )
    .orderBy(F.col("reading_id").asc_nulls_last(), F.col("site_name").asc())
)
