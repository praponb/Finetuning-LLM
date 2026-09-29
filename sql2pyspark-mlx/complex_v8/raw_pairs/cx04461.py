from pyspark.sql import functions as F

cleaned_sites = (
    df_sites
    .filter((F.lower(F.col("site_name")).like("%rm%")) & (F.col("tariff") != "EV"))
    .select(
        "id",
        "site_name",
        "zone",
        "installed_kw",
        "commissioned_on",
    )
)

site_rows = (
    cleaned_sites
    .filter(F.col("commissioned_on") < F.lit("2024-11-21").cast("date"))
    .select("id", "installed_kw", "commissioned_on")
)

site_side = df_sites.select("id", "installed_kw", "commissioned_on")

result = (
    site_rows
    .unionByName(site_side)
    .distinct()
    .orderBy("id")
)
