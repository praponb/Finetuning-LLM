from pyspark.sql import functions as F

base = (
    df_meter_readings
    .filter((F.col("kwh") < 110.5) & (F.col("site_id").isNotNull()))
    .select(
        "id",
        "site_id",
        "read_at",
        "kwh",
        "voltage",
        "quality",
        F.lower(F.trim(F.col("quality"))).alias("quality_clean"),
        F.coalesce(F.col("quality"), F.lit("UNKNOWN")).alias("quality_or_unknown"),
        F.to_date(F.col("read_at")).alias("read_at_date"),
    )
)

combined_details = (
    base.alias("s")
    .join(df_sites.alias("si"), F.col("si.id") == F.col("s.site_id"), "inner")
    .select(
        "s.id",
        "s.site_id",
        "s.read_at",
        "s.quality",
        "s.quality_clean",
        "s.quality_or_unknown",
        "s.read_at_date",
        F.col("si.commissioned_on").alias("site_commissioned_on"),
    )
)

filtered = (
    combined_details.alias("s")
    .join(df_meter_readings.alias("m"), F.col("m.site_id") == F.col("s.site_id"), "left_semi")
    .filter(F.col("read_at_date").between(F.lit("2024-05-01").cast("date"), F.lit("2025-06-01").cast("date")))
)

reading_stats = (
    df_meter_readings
    .groupBy("site_id")
    .agg(F.max("read_at").alias("last_reading_read_at"))
)

profiled = (
    filtered.alias("s")
    .join(reading_stats.alias("m"), F.col("m.site_id") == F.col("s.site_id"), "left")
    .select("s.*", "m.last_reading_read_at")
)

result = (
    profiled
    .select(
        "id",
        "site_id",
        "read_at",
        "quality",
        "quality_clean",
        "quality_or_unknown",
        "read_at_date",
        "site_commissioned_on",
        "last_reading_read_at",
        F.round((F.unix_timestamp(F.col("read_at")) - F.unix_timestamp(F.col("last_reading_read_at"))) / 60, 1).alias("minutes_last_reading_read_at_to_read_at"),
        F.year(F.col("read_at_date")).alias("read_at_date_year"),
    )
    .orderBy("site_commissioned_on", F.col("quality").asc())
)
