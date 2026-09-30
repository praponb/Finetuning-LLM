from pyspark.sql import functions as F

site_side = (
    df_sites
    .filter(F.col("zone") == "Z4")
    .select(
        F.lit("site").alias("source"),
        "id",
        "installed_kw",
        F.col("commissioned_on").alias("event_date"),
    )
)

site_side_2 = (
    df_sites
    .filter(F.col("commissioned_on").isNotNull())
    .select(
        F.lit("site_2").alias("source"),
        "id",
        "installed_kw",
        "commissioned_on",
    )
)

combined = site_side.union(site_side_2)

result = (
    combined
    .groupBy("source")
    .agg(
        F.count("*").alias("n_rows"),
        F.sum("installed_kw").alias("total_installed_kw"),
        F.max("event_date").alias("latest_event_date"),
    )
    .orderBy("source")
)
