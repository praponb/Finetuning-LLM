from pyspark.sql import functions as F

valid_sites = (
    df_sites
    .filter(F.col("installed_kw").between(66.5, 446.9))
    .select(
        "id",
        "zone",
        "tariff",
        "installed_kw",
        "commissioned_on",
    )
)

site_side = valid_sites.select("id", "zone", F.lit("site").alias("source"))

reading_side = (
    df_meter_readings
    .filter((F.col("kwh") > 37.0) & (F.col("site_id") <= 7))
    .select(
        F.col("site_id").alias("id"),
        F.col("quality").alias("zone"),
        F.lit("reading").alias("source"),
    )
)

result = site_side.unionByName(reading_side)
