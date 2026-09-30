from pyspark.sql import functions as F

base = (
    df_meter_readings
    .filter(F.col("read_at") < F.lit("2025-03-02 09:00:00").cast("timestamp"))
    .select(
        "id",
        "site_id",
        "read_at",
        "voltage",
        "quality",
        (F.col("voltage") - F.col("kwh")).alias("voltage_minus_kwh"),
        F.datediff(F.lit("2025-12-31").cast("date"), F.col("read_at")).alias("days_since_read_at"),
        F.lower(F.regexp_replace(F.trim(F.col("quality")), " ", "_")).alias("quality_slug"),
    )
)

joined = (
    base.alias("s")
    .join(df_sites.alias("si"), F.col("si.id") == F.col("s.site_id"), "left")
    .select(
        "s.id",
        "s.site_id",
        "s.voltage",
        "s.voltage_minus_kwh",
        "s.quality_slug",
        "si.site_name",
    )
)

combined_details_site = (
    joined.alias("s")
    .join(df_sites.alias("si"), F.col("si.id") == F.col("s.site_id"), "inner")
    .select(
        "s.id",
        "s.site_id",
        "s.voltage",
        "s.voltage_minus_kwh",
        F.col("si.zone").alias("site_zone"),
    )
)

with_children_readings = (
    combined_details_site.alias("s")
    .join(df_meter_readings.alias("m"), F.col("m.site_id") == F.col("s.site_id"), "inner")
    .select(
        "s.id",
        "s.site_id",
        "s.voltage",
        "s.voltage_minus_kwh",
        F.col("m.id").alias("reading_id"),
        F.col("m.read_at").alias("reading_read_at"),
        F.col("m.voltage").alias("reading_voltage"),
    )
)

result = (
    with_children_readings
    .rollup("reading_id", "site_id")
    .agg(
        F.round(F.stddev("voltage_minus_kwh"), 2).alias("stddev_voltage_minus_kwh"),
    )
)
