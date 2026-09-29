from pyspark.sql import functions as F

cleaned = (
    df_sites
    .filter(F.col("zone").isin("Z4", "Z2"))
    .select(
        "id",
        "site_name",
        "zone",
        "tariff",
        "installed_kw",
        "commissioned_on",
        F.concat_ws(": ", F.col("tariff"), F.col("zone")).alias("tariff_zone_label"),
    )
)

site_side = (
    cleaned
    .filter(F.col("tariff") == "TOU")
    .select(F.col("id").alias("entity_id"))
)

site_side_2 = df_sites.select("id")

result = site_side.subtract(site_side_2)
