from pyspark.sql import functions as F

prepared = (
    df_meter_readings
    .filter(F.month(F.col("read_at")).isin(2, 3))
    .select(
        "id",
        "site_id",
        "read_at",
        "kwh",
        "voltage",
        "quality",
        F.concat(F.col("quality"), F.lit("#"), F.col("id").cast("string")).alias("quality_ref"),
        F.date_trunc("quarter", F.col("read_at")).alias("read_at_quarter"),
        F.lower(F.trim(F.col("quality"))).alias("quality_clean"),
    )
)

combined_details_site = (
    prepared.alias("s")
    .join(df_sites.alias("si"), F.col("si.id") == F.col("s.site_id"), "inner")
    .select(
        "s.*",
        F.col("si.zone").alias("site_zone"),
        F.col("si.commissioned_on").alias("site_commissioned_on"),
        F.col("si.installed_kw").alias("site_installed_kw"),
    )
)

with_details_site = (
    combined_details_site.alias("s")
    .join(df_sites.alias("si"), F.col("si.id") == F.col("s.site_id"), "inner")
    .select("s.*", F.col("si.installed_kw").alias("site_installed_kw_2"))
)

result = (
    with_details_site
    .rollup("site_id", "quality")
    .agg(
        F.grouping("site_id").alias("is_total_site_id"),
        F.round(F.avg("site_installed_kw"), 2).alias("avg_site_installed_kw"),
        F.round(F.sum("site_installed_kw_2"), 2).alias("total_site_installed_kw_2"),
    )
    .orderBy(F.col("site_id").asc_nulls_last(), F.col("quality").asc_nulls_last())
)
