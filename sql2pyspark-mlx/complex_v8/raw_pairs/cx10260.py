from pyspark.sql import functions as F

cleaned_readings = (
    df_meter_readings
    .filter(F.col("quality") != "OK")
    .select(
        "id",
        "site_id",
        "read_at",
        "kwh",
        "quality",
        F.round(F.col("voltage") * 1.07, 2).alias("voltage_adj"),
        F.substring(F.trim(F.col("quality")), 1, 3).alias("quality_prefix"),
        F.date_trunc("month", F.col("read_at")).alias("read_at_month"),
    )
)

enriched = (
    cleaned_readings.alias("s")
    .join(df_sites.alias("si"), F.col("si.id") == F.col("s.site_id"), "left")
    .select(
        "s.id",
        "s.site_id",
        "s.read_at",
        "s.kwh",
        "s.quality_prefix",
        "s.read_at_month",
        F.col("si.commissioned_on").alias("site_commissioned_on"),
        F.col("si.installed_kw").alias("site_installed_kw"),
    )
)

tagged = (
    enriched
    .filter(F.col("kwh") <= 81.3)
    .select(
        "*",
        F.quarter(F.col("site_commissioned_on")).alias("site_commissioned_on_quarter"),
    )
)

result = (
    tagged
    .select(
        "id",
        "site_id",
        "read_at",
        "kwh",
        "quality_prefix",
        "read_at_month",
        "site_installed_kw",
        "site_commissioned_on_quarter",
    )
    .orderBy(F.col("read_at_month").desc_nulls_first())
)
