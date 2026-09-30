from pyspark.sql import functions as F

site_side = (
    df_sites
    .filter(F.col("installed_kw") < 284.0)
    .select(
        F.col("id").alias("entity_id"),
        F.col("commissioned_on").alias("event_date"),
        F.lit("site").alias("record_type"),
    )
)

site_rows = (
    df_sites
    .filter((~F.col("zone").isin("Z2", "Z4")) & (F.col("installed_kw") < 284.0))
    .select("id", "commissioned_on", F.lit("site_2").alias("record_type"))
)

result = site_side.union(site_rows)
