from pyspark.sql import functions as F

recent = (
    df_meter_readings
    .filter(F.col("kwh") < 104.8)
    .select(
        "id",
        "site_id",
        "read_at",
        "kwh",
        "voltage",
        "quality",
    )
)

reading_stats = (
    df_meter_readings
    .groupBy("site_id")
    .agg(
        F.count(F.when(F.col("quality") == "OK", 1)).alias("reading_quality_count"),
    )
)

summarized_rows = (
    recent.alias("s")
    .join(reading_stats.alias("m"), F.col("m.site_id") == F.col("s.site_id"), "left")
    .select(
        "s.*",
        F.coalesce(F.col("m.reading_quality_count"), F.lit(0)).alias("reading_quality_count"),
    )
)

with_parent_site = (
    summarized_rows.alias("s")
    .join(df_sites.alias("si"), F.col("si.id") == F.col("s.site_id"), "inner")
    .select(
        "s.id",
        "s.site_id",
        "s.kwh",
        "s.reading_quality_count",
        F.col("si.zone").alias("site_zone"),
    )
)

result = (
    with_parent_site
    .select(
        "id",
        "kwh",
        "site_zone",
        F.lower(F.regexp_replace(F.trim(F.col("site_zone")), " ", "_")).alias("site_zone_slug"),
        F.when(F.col("site_zone").isin("Z2", "Z3"), "group_a").when(F.col("site_zone") == "Z4", "group_b").otherwise("other").alias("site_zone_group"),
    )
    .orderBy(
        F.col("site_zone_slug").desc_nulls_first(),
        F.col("site_zone").desc_nulls_first(),
    )
)
