from pyspark.sql import functions as F

filtered = (
    df_meter_readings
    .filter((F.col("quality").isin("BAD", "EST")) & (F.month(F.col("read_at")).isin(1, 3)))
    .select(
        "id",
        "site_id",
        "read_at",
        "kwh",
        "voltage",
        "quality",
        F.date_format(F.col("read_at"), "yyyy-MM-dd").alias("read_at_label"),
        F.initcap(F.trim(F.col("quality"))).alias("quality_clean"),
        F.abs(F.col("kwh") - 74.3).alias("kwh_distance"),
    )
)

tagged = (
    filtered
    .filter(F.col("voltage") < 238.3)
    .select(
        "id",
        "site_id",
        "kwh",
        "quality",
        "read_at_label",
        "quality_clean",
        F.abs(F.col("kwh_distance") - 78.0).alias("kwh_distance_distance"),
    )
)

joined = (
    tagged.alias("s")
    .join(df_sites.alias("si"), F.col("si.id") == F.col("s.site_id"), "left")
    .select(
        "s.id",
        "s.site_id",
        "s.kwh",
        "s.read_at_label",
        "s.quality_clean",
        "s.kwh_distance_distance",
        F.col("si.zone").alias("site_zone"),
        "si.site_name",
    )
)

reading_stats = (
    df_meter_readings
    .groupBy("site_id")
    .agg(
        F.count(F.when(F.col("quality") != "EST", 1)).alias("reading_quality_count"),
    )
)

profiled = (
    joined.alias("s")
    .join(reading_stats.alias("m"), F.col("m.site_id") == F.col("s.site_id"), "left")
    .select(
        "s.*",
        F.coalesce(F.col("m.reading_quality_count"), F.lit(0)).alias("reading_quality_count"),
    )
)

result = (
    profiled
    .select(
        "id",
        "site_id",
        "kwh",
        "kwh_distance_distance",
        "site_name",
        "reading_quality_count",
        F.upper(F.trim(F.col("site_zone"))).alias("site_zone_clean"),
    )
    .orderBy("kwh_distance_distance", F.col("kwh").desc())
)
