from pyspark.sql import functions as F

recent = (
    df_meter_readings
    .filter(F.col("site_id") >= 7)
    .select(
        "id",
        "site_id",
        "kwh",
        "voltage",
        "quality",
        F.lower(F.regexp_replace(F.trim(F.col("quality")), " ", "_")).alias("quality_slug"),
    )
)

line_level = (
    recent.alias("s")
    .join(df_meter_readings.alias("m"), F.col("m.site_id") == F.col("s.site_id"), "left")
    .select(
        "s.id",
        "s.site_id",
        "s.quality_slug",
        F.col("m.id").alias("reading_id"),
        F.col("m.kwh").alias("reading_kwh"),
        F.col("m.read_at").alias("reading_read_at"),
        F.col("m.quality").alias("reading_quality"),
    )
)

combined_details = (
    line_level.alias("s")
    .join(df_sites.alias("si"), F.col("si.id") == F.col("s.site_id"), "inner")
    .select(
        "s.id",
        "s.site_id",
        "s.reading_id",
        "s.reading_kwh",
        "s.reading_read_at",
        F.col("si.tariff").alias("site_tariff"),
        F.col("si.zone").alias("site_zone"),
    )
)

tagged = (
    combined_details
    .select(
        "id",
        "site_id",
        "reading_id",
        "reading_kwh",
        F.when(F.col("site_tariff").isin("TOU", "EV"), "group_a").when(F.col("site_tariff") == "FLAT", "group_b").otherwise("other").alias("site_tariff_group"),
        (F.floor(F.col("reading_kwh") / 25) * 25).alias("reading_kwh_bucket"),
    )
)

reading_stats = (
    df_meter_readings
    .groupBy("site_id")
    .agg(
        F.count("*").alias("reading_count"),
        F.count("*").alias("reading_count_2"),
    )
)

with_stats = (
    tagged.alias("s")
    .join(reading_stats.alias("m"), F.col("m.site_id") == F.col("s.site_id"), "left")
    .select(
        "s.*",
        F.coalesce(F.col("m.reading_count"), F.lit(0)).alias("reading_count"),
        F.coalesce(F.col("m.reading_count_2"), F.lit(0)).alias("reading_count_2"),
    )
)

result = (
    with_stats
    .select(
        "id",
        "reading_id",
        F.col("reading_count").alias("m_reading_count"),
        F.col("reading_kwh_bucket").alias("m_reading_kwh_bucket"),
    )
    .unpivot(
        ids=["id", "reading_id"],
        values=["m_reading_count", "m_reading_kwh_bucket"],
        variableColumnName="measure",
        valueColumnName="value",
    )
    .orderBy("id", "reading_id", "measure")
)
