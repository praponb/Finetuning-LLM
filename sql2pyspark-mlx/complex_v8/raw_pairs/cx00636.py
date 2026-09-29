from pyspark.sql import functions as F

prepared = (
    df_meter_readings
    .filter((F.month(F.col("read_at")).isin(1, 3)) & (F.col("quality") == "OK"))
    .select(
        "id",
        "site_id",
        "kwh",
        "voltage",
        "quality",
        F.date_format(F.col("read_at"), "yyyy-MM-dd").alias("read_at_label"),
        F.lower(F.regexp_replace(F.trim(F.col("quality")), " ", "_")).alias("quality_slug"),
        F.date_trunc("hour", F.col("read_at")).alias("read_at_hour"),
    )
)

enriched_site = (
    prepared.alias("s")
    .join(df_sites.alias("si"), F.col("si.id") == F.col("s.site_id"), "left")
    .select(
        "s.id",
        "s.site_id",
        "s.quality",
        "s.read_at_hour",
        F.col("si.commissioned_on").alias("site_commissioned_on"),
        F.col("si.tariff").alias("site_tariff"),
    )
)

labeled = (
    enriched_site
    .select(
        "id",
        "quality",
        "read_at_hour",
        "site_commissioned_on",
        F.coalesce(F.col("quality"), F.lit("UNKNOWN")).alias("quality_or_unknown"),
    )
)

derived = (
    labeled
    .filter(F.col("quality").isNotNull())
    .select(
        "*",
        F.lower(F.regexp_replace(F.trim(F.col("quality_or_unknown")), " ", "_")).alias("quality_or_unknown_slug"),
        F.when(F.col("quality_or_unknown").isin("EST", "OK"), "group_a").when(F.col("quality_or_unknown") == "UNKNOWN", "group_b").otherwise("other").alias("quality_or_unknown_group"),
        F.when(F.col("quality").isin("EST", "BAD"), "group_a").when(F.col("quality") == "OK", "group_b").otherwise("other").alias("quality_group"),
    )
)

result = (
    derived
    .filter(~F.col("quality_or_unknown").isin("EST", "OK"))
    .select(
        "id",
        "quality",
        "read_at_hour",
        "site_commissioned_on",
        "quality_or_unknown",
        "quality_group",
    )
    .orderBy(
        F.col("read_at_hour").desc_nulls_first(),
        F.col("quality_group").asc_nulls_last(),
        "id",
    )
    .limit(20)
)
