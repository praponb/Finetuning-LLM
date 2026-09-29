from pyspark.sql import functions as F

site_rows = (
    df_sites
    .filter((F.col("zone") == "Z3") & (F.col("installed_kw").between(66.5, 373.3)))
    .select(
        F.col("id").alias("ref_id"),
        F.col("commissioned_on").alias("event_date"),
    )
)

site_rows_2 = df_sites.select("id", "commissioned_on")

combined = site_rows.union(site_rows_2)

result = (
    combined
    .groupBy("ref_id")
    .agg(
        F.count("*").alias("n_rows"),
        F.max("event_date").alias("latest_event_date"),
    )
    .orderBy("ref_id")
)
