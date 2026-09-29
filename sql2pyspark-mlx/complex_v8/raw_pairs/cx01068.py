from pyspark.sql import functions as F

site_side = (
    df_sites
    .filter(F.col("commissioned_on").between(F.lit("2024-06-27").cast("date"), F.lit("2025-05-10").cast("date")))
    .select(
        F.col("id").alias("ref_id"),
        F.col("commissioned_on").alias("event_date"),
        "installed_kw",
        F.lit("site").alias("origin"),
    )
)

site_side_2 = (
    df_sites
    .filter((F.col("installed_kw") < 386.6) & (F.col("site_name").isNotNull()))
    .select(
        "id",
        "commissioned_on",
        "installed_kw",
        F.lit("site_2").alias("origin"),
    )
)

unioned = site_side.union(site_side_2)

result = (
    unioned
    .groupBy("origin")
    .agg(
        F.count("*").alias("n_rows"),
        F.sum("installed_kw").alias("total_installed_kw"),
        F.max("event_date").alias("latest_event_date"),
    )
    .orderBy("origin")
)
