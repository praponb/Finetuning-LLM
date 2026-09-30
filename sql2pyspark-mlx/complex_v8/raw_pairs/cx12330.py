from pyspark.sql import functions as F

recent_sites = (
    df_sites
    .filter(F.col("installed_kw") >= 96.9)
    .select(
        "id",
        "site_name",
        "zone",
        "commissioned_on",
        F.concat_ws("|", F.col("zone"), F.col("tariff")).alias("zone_tariff_label"),
    )
)

site_side = (
    df_sites
    .select(
        F.col("id").alias("key_id"),
        F.col("installed_kw").alias("value"),
        F.col("commissioned_on").alias("event_date"),
    )
)

site_side_2 = (
    df_sites
    .select(
        F.col("id").alias("key_id"),
        F.col("installed_kw").alias("value"),
        F.col("commissioned_on").alias("event_date"),
    )
)

result = (
    site_side
    .unionByName(site_side_2)
    .distinct()
    .orderBy("event_date", "key_id")
)
