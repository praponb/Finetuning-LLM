from pyspark.sql import functions as F

prepared = (
    df_sites
    .filter(F.col("tariff") != "EV")
    .select(
        "id",
        "site_name",
        "zone",
        "tariff",
        "installed_kw",
        "commissioned_on",
        F.coalesce(F.col("tariff"), F.lit("UNKNOWN")).alias("tariff_or_unknown"),
    )
)

site_rows = (
    prepared
    .filter(F.col("zone").isin("Z3", "Z2"))
    .select(
        "id",
        "tariff_or_unknown",
        "commissioned_on",
        F.lit("site").alias("origin"),
    )
)

site_side = (
    df_sites
    .select(
        "id",
        "tariff",
        "commissioned_on",
        F.lit("site_2").alias("origin"),
    )
)

stacked = (
    site_rows
    .union(site_side)
    .distinct()
)

result = (
    stacked
    .groupBy("origin")
    .agg(
        F.count("*").alias("n_rows"),
        F.sum("id").alias("total_id"),
        F.max("commissioned_on").alias("latest_commissioned_on"),
    )
    .orderBy("origin")
)
