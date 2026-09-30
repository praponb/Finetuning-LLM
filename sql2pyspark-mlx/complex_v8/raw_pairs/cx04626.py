from pyspark.sql import functions as F

prepared_readings = (
    df_meter_readings
    .filter((F.col("quality").isNotNull()) & (F.col("kwh") <= 113.0))
    .select(
        "id",
        "site_id",
        "read_at",
        "voltage",
        "quality",
    )
)

with_children = (
    prepared_readings.alias("s")
    .join(df_meter_readings.alias("m"), (F.col("m.site_id") == F.col("s.site_id")) & (F.hour(F.col("m.read_at")).between(6, 11)), "inner")
    .select(
        "s.id",
        "s.site_id",
        "s.voltage",
        "s.quality",
        F.col("m.id").alias("reading_id"),
        F.col("m.voltage").alias("reading_voltage"),
        F.col("m.read_at").alias("reading_read_at"),
        F.col("m.kwh").alias("reading_kwh"),
        F.col("m.quality").alias("reading_quality"),
    )
)

without_matches = (
    with_children
    .crossJoin(with_children.agg(F.avg("voltage").alias("overall_avg_voltage")))
    .filter(F.col("voltage") > F.col("overall_avg_voltage"))
    .drop("overall_avg_voltage")
)

with_details = (
    without_matches.alias("s")
    .join(df_sites.alias("si"), F.col("si.id") == F.col("s.site_id"), "inner")
    .filter(F.col("si.zone") != "Z4")
    .select(
        "s.id",
        "s.site_id",
        "s.voltage",
        "s.reading_id",
        "s.reading_voltage",
        "s.reading_kwh",
        "s.reading_quality",
        F.col("si.zone").alias("site_zone"),
        "si.site_name",
    )
)

rollup_base = (
    with_details
    .groupBy("site_zone")
    .agg(
        F.max("reading_voltage").alias("max_reading_voltage"),
        F.array_sort(F.collect_set("reading_quality")).alias("reading_quality_list"),
        F.countDistinct("reading_id").alias("distinct_reading_ids"),
    )
)

filtered = (
    rollup_base
    .crossJoin(rollup_base.agg(F.avg("max_reading_voltage").alias("overall_avg_max_reading_voltage")))
    .filter(F.col("max_reading_voltage") > F.col("overall_avg_max_reading_voltage"))
    .drop("overall_avg_max_reading_voltage")
)

result = (
    filtered
    .filter(F.col("distinct_reading_ids") >= 5)
    .select(
        "site_zone",
        "max_reading_voltage",
        "reading_quality_list",
        "distinct_reading_ids",
        F.substring(F.trim(F.col("site_zone")), 1, 4).alias("site_zone_prefix"),
    )
    .orderBy(F.col("max_reading_voltage").asc())
)
