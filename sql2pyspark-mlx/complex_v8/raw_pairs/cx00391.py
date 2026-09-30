from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_dedup = Window.partitionBy("reading_quality", "site_id").orderBy(F.col("site_commissioned_on").desc(), F.col("id").desc(), F.col("reading_id").desc())

base = (
    df_meter_readings
    .filter((F.col("site_id") >= 5) & (F.col("quality") == "OK"))
    .select(
        "id",
        "site_id",
        "read_at",
        "kwh",
        "voltage",
        "quality",
        F.quarter(F.col("read_at")).alias("read_at_quarter"),
        F.when(F.col("quality").isin("BAD", "EST"), "group_a").when(F.col("quality") == "OK", "group_b").otherwise("other").alias("quality_group"),
        F.abs(F.col("kwh") - 74.3).alias("kwh_distance"),
    )
)

enriched = (
    base
    .filter(F.col("read_at_quarter") > 1)
    .select(
        "*",
        F.round(F.col("kwh_distance") / F.nullif(F.col("voltage"), F.lit(0)), 3).alias("kwh_distance_per_voltage"),
    )
)

reading_stats = (
    df_meter_readings
    .groupBy("site_id")
    .agg(
        F.count("*").alias("reading_count"),
        F.min("kwh").alias("min_reading_kwh"),
    )
)

with_stats = (
    enriched.alias("s")
    .join(reading_stats.alias("m"), F.col("m.site_id") == F.col("s.site_id"), "left")
    .select(
        "s.*",
        F.coalesce(F.col("m.reading_count"), F.lit(0)).alias("reading_count"),
        "m.min_reading_kwh",
    )
)

tagged = (
    with_stats
    .select(
        "id",
        "site_id",
        "read_at",
        "kwh",
        "voltage",
        "quality",
        "read_at_quarter",
        "kwh_distance",
        "kwh_distance_per_voltage",
        "reading_count",
        "min_reading_kwh",
        F.round(F.col("voltage") / F.nullif(F.col("kwh"), F.lit(0)), 3).alias("voltage_per_kwh"),
        F.length(F.trim(F.col("quality_group"))).alias("quality_group_len"),
    )
)

line_level_readings = (
    tagged.alias("s")
    .join(df_meter_readings.alias("m"), (F.col("m.site_id") == F.col("s.site_id")) & (F.col("m.voltage") > 222.6), "inner")
    .select(
        "s.id",
        "s.site_id",
        "s.read_at",
        "s.voltage",
        "s.read_at_quarter",
        "s.kwh_distance",
        "s.kwh_distance_per_voltage",
        "s.reading_count",
        "s.min_reading_kwh",
        "s.voltage_per_kwh",
        F.col("m.id").alias("reading_id"),
        F.col("m.voltage").alias("reading_voltage"),
        F.col("m.quality").alias("reading_quality"),
    )
)

with_parent = (
    line_level_readings.alias("s")
    .join(df_sites.alias("si"), F.col("si.id") == F.col("s.site_id"), "inner")
    .filter(F.year(F.col("si.commissioned_on")) == 2025)
    .select(
        "s.id",
        "s.site_id",
        "s.read_at",
        "s.voltage",
        "s.read_at_quarter",
        "s.kwh_distance",
        "s.kwh_distance_per_voltage",
        "s.voltage_per_kwh",
        "s.reading_id",
        "s.reading_voltage",
        "s.reading_quality",
        F.col("si.commissioned_on").alias("site_commissioned_on"),
        F.col("si.installed_kw").alias("site_installed_kw"),
    )
)

deduped = (
    with_parent
    .withColumn("row_num", F.row_number().over(w_dedup))
    .filter(F.col("row_num") == 1)
    .drop("row_num")
)

result = (
    deduped
    .groupBy("read_at_quarter", "read_at")
    .pivot("reading_quality", ["BAD", "EST", "OK"])
    .agg(F.max("site_installed_kw"))
)
