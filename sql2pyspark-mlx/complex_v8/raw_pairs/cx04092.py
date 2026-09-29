from pyspark.sql import functions as F

prepared_sites = (
    df_sites
    .filter((F.col("commissioned_on").between(F.lit("2024-06-27").cast("date"), F.lit("2025-12-03").cast("date"))) & (F.col("installed_kw") <= 284.0))
    .select(
        "id",
        "site_name",
        "zone",
        "tariff",
        "installed_kw",
        "commissioned_on",
        F.when(F.col("installed_kw") >= 386.6, "A").when(F.col("installed_kw") >= 96.9, "B").otherwise("C").alias("installed_kw_tier"),
        F.date_trunc("month", F.col("commissioned_on")).alias("commissioned_on_month"),
    )
)

with_activity = (
    prepared_sites.alias("s")
    .join(df_meter_readings.alias("m"), (F.col("m.site_id") == F.col("s.id")) & (F.col("m.kwh").between(29.2, 117.2)) & (F.col("m.quality").isNotNull()), "left_anti")
    .filter(F.col("zone").isNotNull())
)

result = (
    with_activity
    .groupingSets(
        [["zone", "tariff"], ["zone"], []],
        "zone", "tariff",
    )
    .agg(
        F.grouping_id().alias("grouping_level"),
        F.round(F.avg("installed_kw"), 2).alias("avg_installed_kw"),
    )
    .orderBy(F.col("zone").asc_nulls_last(), F.col("tariff").asc_nulls_last())
)
