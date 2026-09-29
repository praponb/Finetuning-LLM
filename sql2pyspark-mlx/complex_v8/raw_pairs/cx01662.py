from pyspark.sql import functions as F

filtered_sites = (
    df_sites
    .filter(F.col("tariff").isin("TOU", "EV"))
    .select(
        "id",
        "site_name",
        "zone",
        "tariff",
        "installed_kw",
        "commissioned_on",
        F.upper(F.trim(F.col("site_name"))).alias("site_name_clean"),
        F.abs(F.col("installed_kw") - 96.9).alias("installed_kw_distance"),
    )
)

annotated = (
    filtered_sites
    .select(
        "*",
        F.round(F.col("installed_kw_distance") / F.nullif(F.col("installed_kw"), F.lit(0)), 3).alias("installed_kw_distance_per_installed_kw"),
        F.datediff(F.lit("2025-12-31").cast("date"), F.col("commissioned_on")).alias("days_since_commissioned_on"),
    )
)

totals = (
    annotated
    .groupBy(
        F.date_format("commissioned_on", "yyyy-MM").alias("commissioned_on_ym"),
        "zone",
        "tariff",
    )
    .agg(
        F.count("*").alias("row_count"),
        F.max("commissioned_on").alias("last_commissioned_on"),
        F.array_sort(F.collect_set("zone")).alias("zone_list"),
    )
)

without_matches = (
    totals
    .crossJoin(totals.agg(F.avg("row_count").alias("overall_avg_row_count")))
    .filter(F.col("row_count") > F.col("overall_avg_row_count"))
    .drop("overall_avg_row_count")
)

result = (
    without_matches
    .select(
        "commissioned_on_ym",
        "zone",
        "tariff",
        "row_count",
        "last_commissioned_on",
        "zone_list",
        F.array_join(F.array_sort(F.col("zone_list")), ",").alias("zone_list_text"),
    )
)
