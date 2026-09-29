from pyspark.sql import functions as F

recent = (
    df_sites
    .select(
        "id",
        "site_name",
        "installed_kw",
        "commissioned_on",
        F.date_trunc("quarter", F.col("commissioned_on")).alias("commissioned_on_quarter"),
    )
)

site_rows = (
    recent
    .filter(F.upper(F.trim(F.col("site_name"))).like("R%"))
    .select("id", F.col("installed_kw").alias("amount"), "commissioned_on")
)

site_side = df_sites.select("id", F.col("installed_kw").alias("amount"), "commissioned_on")

result = (
    site_rows
    .unionByName(site_side)
    .orderBy("id")
)
