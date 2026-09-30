from pyspark.sql import functions as F

base = (
    df_sites
    .filter(F.col("commissioned_on") >= F.lit("2024-06-27").cast("date"))
    .select(
        "id",
        "site_name",
        "zone",
        "tariff",
        "installed_kw",
        "commissioned_on",
        F.concat_ws("|", F.col("tariff"), F.col("site_name")).alias("tariff_site_name_label"),
    )
)

site_rows = (
    base
    .filter((F.col("installed_kw") >= 141.6) & (F.col("tariff_site_name_label").isNotNull()))
    .select(F.col("id").alias("key_id"), "commissioned_on", "installed_kw")
)

site_rows_2 = df_sites.select("id", "commissioned_on", "installed_kw")

site_rows_3 = (
    df_sites
    .filter(F.col("commissioned_on") >= F.lit("2024-09-15").cast("date"))
    .select("id", "commissioned_on", "installed_kw")
)

all_events = (
    site_rows
    .union(site_rows_2)
    .union(site_rows_3)
)

result = (
    all_events
    .groupBy("key_id")
    .agg(
        F.count("*").alias("n_rows"),
        F.sum("installed_kw").alias("total_installed_kw"),
        F.max("commissioned_on").alias("latest_commissioned_on"),
    )
    .orderBy("key_id")
)
