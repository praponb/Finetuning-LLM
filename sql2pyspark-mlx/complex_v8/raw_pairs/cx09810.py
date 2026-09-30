from pyspark.sql import functions as F

filtered_sites = (
    df_sites
    .filter((F.col("installed_kw").between(96.9, 446.9)) & (F.col("zone").isin("Z4", "Z3")))
    .select(
        "id",
        "site_name",
        "tariff",
        "installed_kw",
        "commissioned_on",
    )
)

site_rows = filtered_sites.select("id")

tariff_side = (
    df_tariff_windows
    .filter(F.month(F.col("valid_from")).isin(1, 3))
    .select("id")
)

result = (
    site_rows
    .intersect(tariff_side)
    .orderBy("id")
)
