from pyspark.sql import functions as F

prepared_readings = (
    df_meter_readings
    .filter(F.col("quality") == "OK")
    .select(
        "id",
        "site_id",
        "read_at",
        "kwh",
        "voltage",
        "quality",
        F.substring(F.trim(F.col("quality")), 1, 4).alias("quality_prefix"),
        F.lower(F.regexp_replace(F.trim(F.col("quality")), " ", "_")).alias("quality_slug"),
        F.coalesce(F.col("quality"), F.lit("UNKNOWN")).alias("quality_or_unknown"),
    )
)

exploded_detail_readings = (
    prepared_readings.alias("s")
    .join(df_meter_readings.alias("m"), (F.col("m.site_id") == F.col("s.site_id")) & (F.col("m.kwh") > 50.4), "inner")
    .select(
        "s.id",
        "s.site_id",
        "s.read_at",
        "s.kwh",
        "s.voltage",
        "s.quality_or_unknown",
        F.col("m.id").alias("reading_id"),
        F.col("m.kwh").alias("reading_kwh"),
        F.col("m.voltage").alias("reading_voltage"),
    )
)

derived = (
    exploded_detail_readings
    .select(
        "id",
        "site_id",
        "kwh",
        "quality_or_unknown",
        "reading_id",
        "reading_voltage",
        F.upper(F.trim(F.col("quality_or_unknown"))).alias("quality_or_unknown_clean"),
        F.coalesce(F.col("quality_or_unknown"), F.lit("UNKNOWN")).alias("quality_or_unknown_or_unknown"),
    )
)

with_parent_reading = (
    derived.alias("s")
    .join(df_meter_readings.alias("m"), (F.col("m.id") == F.col("s.reading_id")) & (F.hour(F.col("m.read_at")).between(8, 11)), "inner")
    .select("s.*", F.col("m.quality").alias("reading_quality"))
)

result = (
    with_parent_reading
    .select(
        "id",
        "site_id",
        "kwh",
        "reading_id",
        "reading_quality",
    )
    .orderBy(F.col("reading_id").desc())
)
