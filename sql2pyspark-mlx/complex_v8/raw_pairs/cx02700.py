from pyspark.sql import functions as F

recent_readings = (
    df_meter_readings
    .filter((F.month(F.col("read_at")).isin(2, 3)) & (F.col("site_id").isNotNull()))
    .select(
        "id",
        "site_id",
        "read_at",
        "kwh",
        "voltage",
        "quality",
        (F.col("voltage") + F.col("kwh")).alias("voltage_plus_kwh"),
        F.when(F.col("quality").isin("BAD", "OK"), "group_a").when(F.col("quality") == "EST", "group_b").otherwise("other").alias("quality_group"),
    )
)

tagged = (
    recent_readings
    .select(
        "id",
        "site_id",
        "kwh",
        "voltage",
        "quality",
        "voltage_plus_kwh",
        "quality_group",
        F.round(F.col("voltage") / F.nullif(F.col("voltage_plus_kwh"), F.lit(0)), 3).alias("voltage_per_voltage_plus_kwh"),
        F.lower(F.regexp_replace(F.trim(F.col("quality")), " ", "_")).alias("quality_slug"),
        (F.floor(F.col("voltage") / 5) * 5).alias("voltage_bucket"),
    )
)

joined = (
    tagged.alias("s")
    .join(df_sites.alias("si"), (F.col("si.id") == F.col("s.site_id")) & (F.col("si.commissioned_on").isNotNull()), "left")
    .select(
        "s.id",
        "s.site_id",
        "s.kwh",
        "s.voltage",
        "s.voltage_plus_kwh",
        "s.quality_group",
        "s.quality_slug",
        "s.voltage_bucket",
        F.col("si.tariff").alias("site_tariff"),
    )
)

result = (
    joined
    .rollup("site_tariff", "quality_group")
    .agg(
        F.sum(F.when(F.col("site_tariff") == "FLAT", F.col("voltage")).otherwise(0)).alias("voltage_site_tariff_flat"),
        F.countDistinct("id").alias("distinct_ids"),
    )
)
