from pyspark.sql import functions as F

site_side = (
    df_sites
    .select(
        F.col("id").alias("ref_id"),
        F.col("site_name").alias("label"),
        F.col("commissioned_on").alias("event_date"),
        F.lit("site").alias("source"),
    )
)

site_rows = (
    df_sites
    .filter(F.col("zone").isNotNull())
    .select(
        F.col("id").alias("ref_id"),
        F.col("zone").alias("label"),
        F.col("commissioned_on").alias("event_date"),
        F.lit("site_2").alias("source"),
    )
)

site_side_2 = (
    df_sites
    .select(
        F.col("id").alias("ref_id"),
        F.col("tariff").alias("label"),
        F.col("commissioned_on").alias("event_date"),
        F.lit("site_3").alias("source"),
    )
)

result = (
    site_side
    .unionByName(site_rows)
    .unionByName(site_side_2)
    .orderBy("ref_id", "event_date")
)
