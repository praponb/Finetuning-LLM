from pyspark.sql import functions as F

base = (
    df_sites
    .filter(F.col("tariff").isin("EV", "FLAT"))
    .select(
        "id",
        "site_name",
        "zone",
        "tariff",
        "installed_kw",
        "commissioned_on",
    )
)

site_side = base.select(F.col("zone").alias("category"))

site_side_2 = df_sites.select("zone")

result = (
    site_side
    .exceptAll(site_side_2)
    .orderBy(F.col("category").desc())
)
