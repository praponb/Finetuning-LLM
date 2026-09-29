from pyspark.sql import functions as F

site_rows = (
    df_sites
    .filter(F.col("installed_kw") < 386.6)
    .select(
        F.col("id").alias("ref_id"),
        F.col("commissioned_on").alias("event_date"),
    )
)

site_rows_2 = (
    df_sites
    .select(
        F.col("id").alias("ref_id"),
        F.col("commissioned_on").alias("event_date"),
    )
)

result = site_rows.union(site_rows_2)
