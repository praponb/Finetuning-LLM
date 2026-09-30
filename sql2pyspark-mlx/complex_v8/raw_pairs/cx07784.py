from pyspark.sql import functions as F

scoped_readings = (
    df_meter_readings
    .filter((F.to_date(F.col("read_at")) < F.lit("2025-03-08").cast("date")) & (F.col("voltage") >= 223.1))
    .select(
        "id",
        "site_id",
        "read_at",
        "kwh",
        "quality",
        (F.col("voltage") - F.col("kwh")).alias("voltage_minus_kwh"),
    )
)

combined_details_site = (
    scoped_readings.alias("s")
    .join(df_sites.alias("si"), F.col("si.id") == F.col("s.site_id"), "inner")
    .select("s.*", F.col("si.installed_kw").alias("site_installed_kw"))
)

result = (
    combined_details_site
    .cube("site_id", "quality")
    .agg(F.count("*").alias("cnt"))
    .orderBy(F.col("site_id").asc_nulls_last(), F.col("quality").asc_nulls_last())
)
