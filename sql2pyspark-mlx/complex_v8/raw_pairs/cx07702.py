from pyspark.sql import functions as F

cleaned_sites = (
    df_sites
    .select(
        "id",
        "site_name",
        "zone",
        "tariff",
        "installed_kw",
        "commissioned_on",
        F.date_trunc("week", F.col("commissioned_on")).alias("commissioned_on_week"),
        F.length(F.trim(F.col("tariff"))).alias("tariff_len"),
        F.datediff(F.lit("2025-12-31").cast("date"), F.col("commissioned_on")).alias("days_since_commissioned_on"),
    )
)

line_level_readings = (
    cleaned_sites.alias("s")
    .join(df_meter_readings.alias("m"), F.col("m.site_id") == F.col("s.id"), "inner")
    .select(
        "s.id",
        "s.site_name",
        "s.zone",
        "s.commissioned_on_week",
        "s.tariff_len",
        "s.days_since_commissioned_on",
        F.col("m.id").alias("reading_id"),
        F.col("m.quality").alias("reading_quality"),
        F.col("m.voltage").alias("reading_voltage"),
        F.col("m.kwh").alias("reading_kwh"),
        F.col("m.read_at").alias("reading_read_at"),
    )
)

with_parent_reading = (
    line_level_readings.alias("s")
    .join(df_meter_readings.alias("m"), F.col("m.id") == F.col("s.reading_id"), "inner")
    .select("s.*", F.col("m.voltage").alias("reading_voltage_2"))
)

enriched = (
    with_parent_reading
    .filter(F.col("zone") != "Z4")
    .select(
        "*",
        F.when(F.col("reading_quality").isin("OK", "BAD"), "group_a").when(F.col("reading_quality") == "EST", "group_b").otherwise("other").alias("reading_quality_group"),
        F.date_format(F.col("reading_read_at"), "yyyy-MM-dd").alias("reading_read_at_label"),
    )
)

result = (
    enriched
    .groupingSets(
        [["reading_quality_group", "zone", "reading_quality"], ["reading_quality_group"], ["zone"]],
        "reading_quality_group", "zone", "reading_quality",
    )
    .agg(
        F.min("reading_read_at").alias("first_reading_read_at"),
        F.countDistinct("reading_id").alias("distinct_reading_ids"),
    )
    .orderBy(
        F.col("reading_quality_group").asc_nulls_last(),
        F.col("zone").asc_nulls_last(),
        F.col("reading_quality").asc_nulls_last(),
    )
)
