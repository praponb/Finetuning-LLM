from pyspark.sql import functions as F

prepared_sites = (
    df_sites
    .filter(F.col("commissioned_on") >= F.lit("2024-10-28").cast("date"))
    .select(
        "id",
        "site_name",
        "zone",
        "installed_kw",
    )
)

site_rows = (
    prepared_sites
    .filter((F.upper(F.trim(F.col("site_name"))).like("S%")) & (~F.col("zone").isin("Z2", "Z4")))
    .select(F.col("id").alias("key_id"), "installed_kw")
)

reading_rows = (
    df_meter_readings
    .filter(F.col("voltage") > 215.6)
    .select(
        F.col("site_id").alias("key_id"),
        F.col("voltage").alias("installed_kw"),
    )
)

site_side = (
    df_sites
    .filter(F.col("site_name").isNotNull())
    .select(F.col("id").alias("key_id"), "installed_kw")
)

result = (
    site_rows
    .unionByName(reading_rows)
    .unionByName(site_side)
)
