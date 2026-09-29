from pyspark.sql import functions as F

scoped_sites = (
    df_sites
    .filter(F.col("commissioned_on") >= F.lit("2024-10-28").cast("date"))
    .select(
        "id",
        "site_name",
        "zone",
        "installed_kw",
        F.add_months(F.col("commissioned_on"), 6).alias("commissioned_on_plus_6m"),
    )
)

site_side = (
    scoped_sites
    .filter((F.col("commissioned_on_plus_6m") >= F.lit("2024-12-01").cast("date")) & (F.col("zone").isNotNull()))
    .select(
        F.col("id").alias("ref_id"),
        F.col("commissioned_on_plus_6m").alias("day"),
    )
)

site_side_2 = (
    df_sites
    .filter((F.col("commissioned_on") < F.lit("2025-05-07").cast("date")) & (F.col("zone") != "Z3"))
    .select("id", "commissioned_on")
)

site_side_3 = (
    df_sites
    .filter((~F.col("tariff").isin("TOU", "FLAT")) & (F.col("zone") != "Z4"))
    .select("id", "commissioned_on")
)

stacked = (
    site_side
    .union(site_side_2)
    .union(site_side_3)
)

result = (
    stacked
    .groupBy("ref_id")
    .agg(F.count("*").alias("n_rows"), F.max("day").alias("latest_day"))
    .orderBy("ref_id")
)
