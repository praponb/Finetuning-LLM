from pyspark.sql import functions as F

prepared = (
    df_meter_readings
    .select(
        "id",
        "site_id",
        "read_at",
        "kwh",
        "voltage",
        "quality",
        F.substring(F.trim(F.col("quality")), 1, 2).alias("quality_prefix"),
        F.length(F.trim(F.col("quality"))).alias("quality_len"),
        F.least(F.col("voltage"), F.col("kwh")).alias("least_voltage_kwh"),
    )
)

labeled = (
    prepared
    .filter(F.col("kwh") >= 20.3)
    .select(
        "*",
        F.datediff(F.lit("2025-06-30").cast("date"), F.col("read_at")).alias("days_since_read_at"),
        F.when(F.col("quality_len") >= 10, "A").when(F.col("quality_len") >= 4, "B").otherwise("C").alias("quality_len_tier"),
        F.concat_ws("|", F.col("quality_prefix"), F.col("quality")).alias("quality_prefix_quality_label"),
    )
)

qualified = (
    labeled.alias("s")
    .join(df_meter_readings.alias("m"), F.col("m.site_id") == F.col("s.site_id"), "left_semi")
)

enriched = (
    qualified
    .select(
        "id",
        "site_id",
        "read_at",
        "voltage",
        "quality",
        "quality_prefix",
        "least_voltage_kwh",
        "days_since_read_at",
        "quality_prefix_quality_label",
        F.year(F.col("read_at")).alias("read_at_year"),
        (F.col("kwh") + F.col("quality_len")).alias("kwh_plus_quality_len"),
    )
)

with_details = (
    enriched.alias("s")
    .join(df_sites.alias("si"), F.col("si.id") == F.col("s.site_id"), "inner")
    .select(
        "s.*",
        "si.site_name",
        F.col("si.commissioned_on").alias("site_commissioned_on"),
    )
)

result = (
    with_details
    .cube("site_id", "quality", "read_at_year")
    .agg(
        F.grouping_id().alias("grouping_level"),
        F.round(F.stddev("days_since_read_at"), 2).alias("stddev_days_since_read_at"),
    )
    .select(
        "site_id",
        F.coalesce(F.col("quality"), F.lit("ALL")).alias("quality"),
        "read_at_year",
        "grouping_level",
        "stddev_days_since_read_at",
    )
    .orderBy(
        F.col("site_id").asc_nulls_last(),
        F.col("quality").asc_nulls_last(),
        F.col("read_at_year").asc_nulls_last(),
    )
)
