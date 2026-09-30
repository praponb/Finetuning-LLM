from pyspark.sql import functions as F

base = (
    df_meter_readings
    .select(
        "id",
        "site_id",
        "read_at",
        "voltage",
        "quality",
        F.round(F.col("voltage") * 1.5, 2).alias("voltage_adj"),
        F.lower(F.regexp_replace(F.trim(F.col("quality")), " ", "_")).alias("quality_slug"),
        (F.floor(F.col("kwh") / 25) * 25).alias("kwh_bucket"),
    )
)

quality_stats = (
    base
    .groupBy("quality")
    .agg(F.max("kwh_bucket").alias("max_kwh_bucket"))
)

above_average = (
    base.alias("s")
    .join(quality_stats.alias("g"), F.col("g.quality") == F.col("s.quality"), "inner")
    .filter(F.col("s.kwh_bucket") == F.col("g.max_kwh_bucket"))
    .select(
        "s.*",
        "g.max_kwh_bucket",
        F.round(F.col("s.kwh_bucket") - F.col("g.max_kwh_bucket"), 2).alias("kwh_bucket_minus_max"),
    )
)

derived = (
    above_average
    .filter(F.col("quality_slug").isNotNull())
    .select(
        "*",
        F.round(F.col("kwh_bucket_minus_max") * 1.07, 2).alias("kwh_bucket_minus_max_adj"),
        F.concat(F.col("quality"), F.lit("#"), F.col("kwh_bucket").cast("string")).alias("quality_ref"),
        F.least(F.col("kwh_bucket_minus_max"), F.col("voltage")).alias("least_kwh_bucket_minus_max_voltage"),
    )
)

screened = (
    derived.alias("s")
    .join(df_meter_readings.alias("m"), (F.col("m.site_id") == F.col("s.site_id")) & (F.col("m.read_at") < F.lit("2025-02-18 12:00:00").cast("timestamp")) & (F.col("m.quality") != "BAD"), "left_semi")
)

tagged = (
    screened
    .filter(F.col("max_kwh_bucket").between(12.0, 90.0))
    .withColumn("voltage_adj_adj", F.round(F.col("voltage_adj") * 1.1, 2))
)

combined_details_site = (
    tagged.alias("s")
    .join(df_sites.alias("si"), F.col("si.id") == F.col("s.site_id"), "inner")
    .filter(F.col("si.commissioned_on") >= F.lit("2024-09-15").cast("date"))
    .select(
        "s.id",
        "s.site_id",
        "s.read_at",
        "s.voltage",
        "s.quality",
        "s.voltage_adj",
        "s.max_kwh_bucket",
        "s.kwh_bucket_minus_max",
        "s.kwh_bucket_minus_max_adj",
        "s.quality_ref",
        "s.least_kwh_bucket_minus_max_voltage",
        F.col("si.commissioned_on").alias("site_commissioned_on"),
    )
)

result = (
    combined_details_site
    .select(
        "id",
        "site_id",
        "quality",
        "voltage_adj",
        "max_kwh_bucket",
        "quality_ref",
        F.coalesce(F.col("quality"), F.lit("UNKNOWN")).alias("quality_or_unknown"),
        F.round(F.col("kwh_bucket_minus_max") * 1.07, 2).alias("kwh_bucket_minus_max_adj_2"),
    )
    .orderBy(F.col("quality_ref").asc_nulls_last())
)
