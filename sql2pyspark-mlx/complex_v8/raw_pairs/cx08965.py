from pyspark.sql import functions as F

cleaned_sites = (
    df_sites
    .filter(F.col("zone") == "Z2")
    .select(
        "id",
        "site_name",
        "tariff",
        "commissioned_on",
        F.date_trunc("month", F.col("commissioned_on")).alias("commissioned_on_month"),
        F.when(F.col("zone").isin("Z3", "Z2"), "group_a").when(F.col("zone") == "Z4", "group_b").otherwise("other").alias("zone_group"),
    )
)

labeled = (
    cleaned_sites
    .filter(F.col("zone_group").isin("group_b", "group_a"))
    .select(
        "*",
        F.datediff(F.lit("2026-01-01").cast("date"), F.col("commissioned_on_month")).alias("days_since_commissioned_on_month"),
        F.lower(F.regexp_replace(F.trim(F.col("zone_group")), " ", "_")).alias("zone_group_slug"),
        F.length(F.trim(F.col("zone_group"))).alias("zone_group_len"),
    )
)

qualified = (
    labeled
    .crossJoin(labeled.agg(F.min("zone_group_len").alias("overall_min_zone_group_len")))
    .filter(F.col("zone_group_len") > F.col("overall_min_zone_group_len"))
    .drop("overall_min_zone_group_len")
)

reading_stats = (
    df_meter_readings
    .groupBy("site_id")
    .agg(F.max("read_at").alias("last_reading_read_at"))
)

with_stats = (
    qualified.alias("s")
    .join(reading_stats.alias("m"), F.col("m.site_id") == F.col("s.id"), "left")
    .select("s.*", "m.last_reading_read_at")
)

result = (
    with_stats
    .select(
        "id",
        "tariff",
        "commissioned_on_month",
        "zone_group",
        "last_reading_read_at",
        F.hour(F.col("commissioned_on_month")).alias("commissioned_on_month_hour"),
        F.coalesce(F.col("zone_group"), F.lit("UNKNOWN")).alias("zone_group_or_unknown"),
    )
    .orderBy(F.col("commissioned_on_month").desc())
)
