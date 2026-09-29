from pyspark.sql import functions as F

base = (
    df_sites
    .filter((F.length(F.trim(F.col("site_name"))) >= 8) & (F.year(F.col("commissioned_on")) == 2024))
    .select(
        "id",
        "site_name",
        "zone",
        "tariff",
        "installed_kw",
        "commissioned_on",
        F.concat(F.col("tariff"), F.lit("#"), F.col("id").cast("string")).alias("tariff_ref"),
    )
)

site_rows = (
    base
    .filter(~F.col("tariff").isin("TOU", "FLAT"))
    .select(F.col("id").alias("qty"))
)

site_rows_2 = df_sites.select("id")

result = (
    site_rows
    .intersect(site_rows_2)
    .orderBy(F.col("qty").desc())
)
