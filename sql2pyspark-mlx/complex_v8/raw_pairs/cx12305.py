from pyspark.sql import functions as F

scoped = (
    df_sites
    .filter((F.col("tariff") == "FLAT") & (F.upper(F.trim(F.col("site_name"))).like("S%")))
    .select(
        "id",
        "site_name",
        "zone",
        "tariff",
        "installed_kw",
        "commissioned_on",
        F.datediff(F.lit("2025-06-30").cast("date"), F.col("commissioned_on")).alias("days_since_commissioned_on"),
        F.quarter(F.col("commissioned_on")).alias("commissioned_on_quarter"),
        F.datediff(F.lit("2025-06-30").cast("date"), F.col("commissioned_on")).alias("days_since_commissioned_on_2"),
    )
)

screened = (
    scoped
    .crossJoin(scoped.agg(F.avg("commissioned_on_quarter").alias("overall_avg_commissioned_on_quarter")))
    .filter(F.col("commissioned_on_quarter") > F.col("overall_avg_commissioned_on_quarter"))
    .drop("overall_avg_commissioned_on_quarter")
)

tagged = (
    screened
    .select(
        "id",
        "site_name",
        "zone",
        "commissioned_on",
        "days_since_commissioned_on",
        F.greatest(F.col("commissioned_on_quarter"), F.col("installed_kw")).alias("greatest_commissioned_on_quarter_installed_kw"),
        F.substring(F.trim(F.col("site_name")), 1, 2).alias("site_name_prefix"),
    )
)

result = (
    tagged
    .select(
        "id",
        "site_name",
        "zone",
        "commissioned_on",
        "days_since_commissioned_on",
        "site_name_prefix",
        F.trunc(F.col("commissioned_on"), "MM").alias("commissioned_on_month_start"),
        F.coalesce(F.col("zone"), F.lit("UNKNOWN")).alias("zone_or_unknown"),
    )
    .orderBy(F.col("site_name_prefix").asc(), F.col("commissioned_on").desc())
)
