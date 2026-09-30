from pyspark.sql import functions as F

base_readings = (
    df_meter_readings
    .filter(F.to_date(F.col("read_at")) < F.lit("2025-02-18").cast("date"))
    .select(
        "id",
        "site_id",
        "read_at",
        "kwh",
        "voltage",
        "quality",
        F.length(F.trim(F.col("quality"))).alias("quality_len"),
    )
)

tagged = (
    base_readings
    .select(
        "*",
        F.date_format(F.col("read_at"), "yyyy-MM").alias("read_at_label"),
        F.substring(F.trim(F.col("quality")), 1, 4).alias("quality_prefix"),
        F.lower(F.regexp_replace(F.trim(F.col("quality")), " ", "_")).alias("quality_slug"),
    )
)

combined_details = (
    tagged.alias("s")
    .join(df_sites.alias("si"), F.col("si.id") == F.col("s.site_id"), "inner")
    .filter(F.col("si.tariff") == "FLAT")
    .select(
        "s.id",
        "s.site_id",
        "s.read_at",
        "s.kwh",
        "s.quality_len",
        "s.quality_slug",
        F.col("si.tariff").alias("site_tariff"),
        F.col("si.commissioned_on").alias("site_commissioned_on"),
        F.col("si.installed_kw").alias("site_installed_kw"),
    )
)

annotated = (
    combined_details
    .withColumn("site_tariff_or_unknown", F.coalesce(F.col("site_tariff"), F.lit("UNKNOWN")))
)

result = (
    annotated
    .groupBy("site_commissioned_on", "read_at")
    .pivot("site_tariff_or_unknown", ["TOU", "UNKNOWN", "FLAT"])
    .agg(F.max("site_installed_kw"))
    .orderBy("site_commissioned_on", "read_at")
)
