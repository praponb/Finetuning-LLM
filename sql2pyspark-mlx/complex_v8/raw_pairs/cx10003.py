from pyspark.sql import functions as F

matched = (
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
        F.sum("b.kwh").alias("prior_kwh_3d"),
    )
)

with_details_site = (
    matched.alias("s")
    .join(df_sites.alias("si"), (F.col("si.id") == F.col("s.site_id")) & (F.col("si.site_name").isNotNull()), "inner")
    .filter(F.length(F.trim(F.col("si.site_name"))) >= 5)
    .select(
        "s.id",
        "s.site_id",
        "s.prior_kwh_3d",
        F.col("si.commissioned_on").alias("site_commissioned_on"),
        F.col("si.tariff").alias("site_tariff"),
        "si.site_name",
    )
)

enriched_site = (
    with_details_site.alias("s")
    .join(df_sites.alias("si"), F.col("si.id") == F.col("s.site_id"), "inner")
    .select(
        "s.id",
        "s.site_id",
        "s.prior_kwh_3d",
        "s.site_commissioned_on",
        "s.site_name",
        F.col("si.installed_kw").alias("site_installed_kw"),
        F.col("si.site_name").alias("site_site_name"),
        F.col("si.tariff").alias("site_tariff_2"),
    )
)

without_matches = (
    enriched_site.alias("s")
    .join(df_meter_readings.alias("m"), F.col("m.site_id") == F.col("s.site_id"), "left_semi")
)

result = (
    without_matches
    .select("id", "prior_kwh_3d", "site_installed_kw")
    .unpivot(
        ids=["id"],
        values=["prior_kwh_3d", "site_installed_kw"],
        variableColumnName="metric_name",
        valueColumnName="amount",
    )
)
