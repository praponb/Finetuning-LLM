from pyspark.sql import functions as F

cleaned = (
    df_sites
    .select(
        "id",
        "site_name",
        "zone",
        "tariff",
        "commissioned_on",
        F.length(F.trim(F.col("zone"))).alias("zone_len"),
        F.upper(F.trim(F.col("zone"))).alias("zone_clean"),
        F.date_format(F.col("commissioned_on"), "MMM yyyy").alias("commissioned_on_label"),
    )
)

without_matches = (
    cleaned
    .crossJoin(cleaned.agg(F.min("zone_len").alias("overall_min_zone_len")))
    .filter(F.col("zone_len") > F.col("overall_min_zone_len"))
    .drop("overall_min_zone_len")
)

result = (
    without_matches
    .select(
        "id",
        "zone",
        "tariff",
        "commissioned_on",
        "zone_len",
    )
)
