from pyspark.sql import functions as F

valid = (
    df_sites
    .filter((F.col("site_name").isNotNull()) & (F.col("zone").isin("Z4", "Z2")))
    .select(
        "id",
        "site_name",
        "zone",
        "installed_kw",
        "commissioned_on",
        F.quarter(F.col("commissioned_on")).alias("commissioned_on_quarter"),
    )
)

reading_stats = (
    df_meter_readings
    .groupBy("site_id")
    .agg(
        F.max("voltage").alias("max_reading_voltage"),
        F.count("*").alias("reading_count"),
    )
)

with_counts = (
    valid.alias("s")
    .join(reading_stats.alias("m"), F.col("m.site_id") == F.col("s.id"), "left")
    .select(
        "s.*",
        "m.max_reading_voltage",
        F.coalesce(F.col("m.reading_count"), F.lit(0)).alias("reading_count"),
    )
)

without_matches = (
    with_counts.alias("s")
    .join(df_meter_readings.alias("m"), (F.col("m.site_id") == F.col("s.id")) & (F.col("m.voltage") >= 215.7), "left_anti")
)

result = (
    without_matches
    .select(
        "id",
        "zone",
        "commissioned_on",
        "max_reading_voltage",
        "reading_count",
        F.when(F.col("reading_count") >= 7, "HIGH").when(F.col("reading_count") >= 3, "MEDIUM").otherwise("LOW").alias("reading_count_tier"),
        (F.col("reading_count") - F.col("commissioned_on_quarter")).alias("reading_count_minus_commissioned_on_quarter"),
    )
    .orderBy(
        F.col("reading_count_minus_commissioned_on_quarter").asc_nulls_last(),
        "id",
    )
)
