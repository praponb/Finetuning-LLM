from pyspark.sql import functions as F

covered = (
    df_meter_readings.alias("a")
    .join(
        df_meter_readings.alias("b"),
        (F.col("b.site_id") == F.col("a.site_id"))
        & (F.col("b.read_at") < F.col("a.read_at"))
        & (F.col("b.read_at") >= F.col("a.read_at") - F.expr("INTERVAL 3 DAYS")),
        "left",
    )
    .groupBy("a.id", "a.site_id", "a.read_at")
    .agg(
        F.count("b.id").alias("prior_rows_3d"),
        F.sum("b.voltage").alias("prior_voltage_3d"),
    )
)

combined_details_site = (
    covered.alias("s")
    .join(df_sites.alias("si"), F.col("si.id") == F.col("s.site_id"), "inner")
    .filter(F.length(F.trim(F.col("si.site_name"))) >= 5)
    .select(
        "s.id",
        "s.site_id",
        "s.read_at",
        "s.prior_voltage_3d",
        F.col("si.zone").alias("site_zone"),
        "si.site_name",
        F.col("si.installed_kw").alias("site_installed_kw"),
    )
)

result = (
    combined_details_site
    .filter(F.col("site_id").isNotNull())
    .select(
        "id",
        "read_at",
        "prior_voltage_3d",
        "site_name",
        "site_installed_kw",
        F.coalesce(F.col("site_zone"), F.lit("UNKNOWN")).alias("site_zone_or_unknown"),
        F.when(F.col("site_installed_kw") >= 386.6, "HIGH").when(F.col("site_installed_kw") >= 141.6, "MEDIUM").otherwise("LOW").alias("site_installed_kw_tier"),
    )
    .orderBy(
        F.col("site_installed_kw_tier").desc(),
        F.col("site_installed_kw").desc_nulls_first(),
    )
)
