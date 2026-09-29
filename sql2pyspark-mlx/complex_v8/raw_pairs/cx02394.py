from pyspark.sql import functions as F

scoped_sites = (
    df_sites
    .filter(F.upper(F.trim(F.col("site_name"))).like("M%"))
    .select(
        "id",
        "zone",
        "tariff",
        "installed_kw",
        "commissioned_on",
        (F.floor(F.col("installed_kw") / 100) * 100).alias("installed_kw_bucket"),
    )
)

site_rows = scoped_sites.select(F.col("id").alias("entity_id"))

tariff_side = df_tariff_windows.select("id")

result = (
    site_rows
    .intersect(tariff_side)
    .orderBy("entity_id")
)
