from pyspark.sql import functions as F

base_sites = (
    df_sites
    .filter(F.col("commissioned_on") < F.lit("2025-05-10").cast("date"))
    .select(
        "id",
        "site_name",
        "zone",
        "installed_kw",
        F.upper(F.trim(F.col("zone"))).alias("zone_clean"),
    )
)

site_rows = (
    base_sites
    .filter((F.length(F.trim(F.col("zone_clean"))) >= 6) & (F.upper(F.trim(F.col("site_name"))).like("H%")))
    .select("site_name", "zone")
)

site_rows_2 = df_sites.select("tariff", "zone")

all_events = (
    site_rows
    .union(site_rows_2)
    .distinct()
)

result = (
    all_events
    .groupBy("site_name")
    .agg(F.count("*").alias("n_rows"))
    .orderBy("site_name")
)
