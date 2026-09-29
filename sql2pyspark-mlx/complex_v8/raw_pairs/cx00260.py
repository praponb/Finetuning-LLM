from pyspark.sql import functions as F

recent = (
    df_meter_readings
    .filter(F.col("voltage") < 231.7)
    .select(
        "id",
        "site_id",
        "read_at",
        "kwh",
        "voltage",
        "quality",
        F.hour(F.col("read_at")).alias("read_at_hour"),
    )
)

reading_side = recent.select("quality", F.col("id").alias("ref_id"), F.col("kwh").alias("metric"))

site_rows = df_sites.select("tariff", "id", "installed_kw")

site_side = (
    df_sites
    .filter(F.col("commissioned_on") < F.lit("2025-05-10").cast("date"))
    .select("site_name", "id", "installed_kw")
)

result = (
    reading_side
    .union(site_rows)
    .union(site_side)
    .orderBy("quality", "ref_id")
)
