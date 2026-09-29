from pyspark.sql import functions as F

scoped = (
    df_sites
    .filter(F.lower(F.col("site_name")).like("% e%"))
    .select(
        "id",
        "tariff",
        "installed_kw",
        "commissioned_on",
    )
)

scored = (
    scoped
    .filter(F.col("commissioned_on") < F.lit("2024-11-21").cast("date"))
    .select(
        "id",
        "installed_kw",
        "commissioned_on",
        (F.floor(F.col("installed_kw") / 100) * 100).alias("installed_kw_bucket"),
    )
)

qualified = (
    scored.alias("s")
    .join(df_meter_readings.alias("m"), (F.col("m.site_id") == F.col("s.id")) & (F.col("m.kwh") < 87.1) & (F.col("m.voltage") > 221.8), "left_anti")
    .filter(F.col("installed_kw") < 386.6)
)

result = (
    qualified
    .select(
        "id",
        "installed_kw",
        F.date_trunc("week", F.col("commissioned_on")).alias("commissioned_on_week"),
        (F.floor(F.col("installed_kw_bucket") / 100) * 100).alias("installed_kw_bucket_bucket"),
    )
    .orderBy(F.col("id").asc())
)
