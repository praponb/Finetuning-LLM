from pyspark.sql import functions as F

cleaned = (
    df_sites
    .select(
        "id",
        "site_name",
        "zone",
        "tariff",
        "installed_kw",
        F.length(F.trim(F.col("tariff"))).alias("tariff_len"),
    )
)

site_rows = df_sites.select(F.col("id").alias("key_id"), "commissioned_on")

site_rows_2 = (
    df_sites
    .filter(F.length(F.trim(F.col("site_name"))) >= 6)
    .select(F.col("id").alias("key_id"), "commissioned_on")
)

result = (
    site_rows
    .unionByName(site_rows_2)
    .orderBy(F.col("commissioned_on").desc())
)
