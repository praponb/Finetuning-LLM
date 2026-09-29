from pyspark.sql import functions as F

prepared = (
    df_meter_readings
    .filter(F.col("kwh") >= 45.3)
    .select(
        "id",
        "site_id",
        "read_at",
        "kwh",
        "voltage",
        "quality",
        F.concat(F.col("quality"), F.lit("#"), F.col("site_id").cast("string")).alias("quality_ref"),
    )
)

qualified = (
    prepared.alias("s")
    .join(df_sites.alias("si"), (F.col("si.id") == F.col("s.site_id")) & (F.col("si.commissioned_on") < F.lit("2025-05-07").cast("date")) & (F.col("si.tariff").isNotNull()), "left_anti")
)

with_children = (
    qualified.alias("s")
    .join(df_meter_readings.alias("m"), F.col("m.site_id") == F.col("s.site_id"), "left")
    .select(
        "s.id",
        "s.site_id",
        "s.kwh",
        "s.quality",
        F.col("m.id").alias("reading_id"),
        F.col("m.voltage").alias("reading_voltage"),
        F.col("m.read_at").alias("reading_read_at"),
        F.col("m.quality").alias("reading_quality"),
        F.col("m.kwh").alias("reading_kwh"),
    )
)

qualified_2 = (
    with_children
    .crossJoin(with_children.agg(F.min("reading_voltage").alias("overall_min_reading_voltage")))
    .filter(F.col("reading_voltage") > F.col("overall_min_reading_voltage"))
    .drop("overall_min_reading_voltage")
)

without_matches = (
    qualified_2.alias("s")
    .join(df_meter_readings.alias("m"), (F.col("m.site_id") == F.col("s.site_id")) & (F.col("m.voltage") <= 236.7) & (F.col("m.quality") == "BAD"), "left_anti")
)

result = (
    without_matches
    .filter(F.col("reading_kwh").between(42.1, 92.5))
    .select(
        "id",
        "site_id",
        "kwh",
        "quality",
        "reading_id",
        "reading_read_at",
        "reading_quality",
        "reading_kwh",
        F.substring(F.trim(F.col("quality")), 1, 2).alias("quality_prefix"),
    )
    .orderBy(F.col("id").desc(), "reading_id")
    .limit(10)
)
