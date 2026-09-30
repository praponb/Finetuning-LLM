from pyspark.sql import functions as F

filtered_sites = (
    df_sites
    .filter((F.col("tariff").isNotNull()) & (F.col("site_name").isNotNull()))
    .select(
        "id",
        "site_name",
        "zone",
        "tariff",
        "installed_kw",
        "commissioned_on",
        F.when(F.col("installed_kw") >= 386.6, "A").when(F.col("installed_kw") >= 141.6, "B").otherwise("C").alias("installed_kw_tier"),
        F.date_trunc("year", F.col("commissioned_on")).alias("commissioned_on_year"),
    )
)

qualified = (
    filtered_sites.alias("s")
    .join(df_meter_readings.alias("m"), (F.col("m.site_id") == F.col("s.id")) & (~F.col("m.quality").isin("EST", "BAD")) & (F.hour(F.col("m.read_at")).between(8, 12)), "left_anti")
    .filter(F.month(F.col("commissioned_on_year")).isin(1, 3))
)

result = (
    qualified
    .select(
        "id",
        "zone",
        "tariff",
        "installed_kw",
        "installed_kw_tier",
        F.datediff(F.lit("2026-01-01").cast("date"), F.col("commissioned_on_year")).alias("days_since_commissioned_on_year"),
    )
    .orderBy("zone", F.col("installed_kw_tier").desc())
)
