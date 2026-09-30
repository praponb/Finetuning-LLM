from pyspark.sql import functions as F

cleaned_readings = (
    df_meter_readings
    .filter(F.col("voltage").between(213.5, 234.7))
    .select(
        "id",
        "site_id",
        "read_at",
        "kwh",
        "voltage",
        "quality",
        F.abs(F.col("voltage") - 225.2).alias("voltage_distance"),
        F.when(F.col("quality").isin("OK", "EST"), "group_a").when(F.col("quality") == "BAD", "group_b").otherwise("other").alias("quality_group"),
    )
)

with_parent = (
    cleaned_readings.alias("s")
    .join(df_sites.alias("si"), F.col("si.id") == F.col("s.site_id"), "inner")
    .select(
        "s.*",
        F.col("si.zone").alias("site_zone"),
        "si.site_name",
        F.col("si.commissioned_on").alias("site_commissioned_on"),
    )
)

result = (
    with_parent
    .rollup("site_zone", "quality_group", "quality")
    .agg(
        F.grouping("site_zone").alias("is_total_site_zone"),
        F.sum("voltage_distance").alias("total_voltage_distance"),
    )
    .select(
        F.coalesce(F.col("site_zone"), F.lit("ALL")).alias("site_zone"),
        F.coalesce(F.col("quality_group"), F.lit("ALL")).alias("quality_group"),
        F.coalesce(F.col("quality"), F.lit("ALL")).alias("quality"),
        "is_total_site_zone",
        "total_voltage_distance",
    )
    .orderBy(
        F.col("site_zone").asc_nulls_last(),
        F.col("quality_group").asc_nulls_last(),
        F.col("quality").asc_nulls_last(),
    )
)
