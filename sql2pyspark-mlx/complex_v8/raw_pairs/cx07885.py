from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_commissioned_on_year_zone_or = Window.partitionBy(F.year("commissioned_on"), "zone_or_unknown").orderBy(F.col("commissioned_on").desc())
w_commissioned_on_year_zone_or_2 = Window.partitionBy(F.year("commissioned_on"), "zone_or_unknown").orderBy(F.col("commissioned_on").desc(), "id")

valid_sites = (
    df_sites
    .filter((F.col("tariff").isin("TOU", "EV")) & (F.upper(F.trim(F.col("site_name"))).like("S%")))
    .select(
        "id",
        "site_name",
        "zone",
        "tariff",
        "commissioned_on",
        F.lower(F.regexp_replace(F.trim(F.col("zone")), " ", "_")).alias("zone_slug"),
        F.when(F.col("installed_kw") >= 373.3, "HIGH").when(F.col("installed_kw") >= 141.6, "MEDIUM").otherwise("LOW").alias("installed_kw_tier"),
        F.coalesce(F.col("zone"), F.lit("UNKNOWN")).alias("zone_or_unknown"),
    )
)

with_trends = (
    valid_sites
    .select(
        "id",
        "site_name",
        "tariff",
        "commissioned_on",
        "zone_slug",
        F.round(F.cume_dist().over(w_commissioned_on_year_zone_or), 3).alias("commissioned_on_cume_dist"),
        F.round(F.cume_dist().over(w_commissioned_on_year_zone_or_2), 3).alias("commissioned_on_cume_dist_2"),
    )
)

reading_stats = (
    df_meter_readings
    .groupBy("site_id")
    .agg(
        F.count(F.when(F.col("quality") == "EST", 1)).alias("reading_quality_count"),
    )
)

summarized_rows = (
    with_trends.alias("s")
    .join(reading_stats.alias("m"), F.col("m.site_id") == F.col("s.id"), "left")
    .select(
        "s.*",
        F.coalesce(F.col("m.reading_quality_count"), F.lit(0)).alias("reading_quality_count"),
    )
)

result = (
    summarized_rows
    .cube("tariff", "reading_quality_count")
    .agg(
        F.grouping("tariff").alias("is_total_tariff"),
        F.sum("commissioned_on_cume_dist").alias("total_commissioned_on_cume_dist"),
        F.countDistinct("id").alias("distinct_ids"),
        F.min("commissioned_on").alias("first_commissioned_on"),
    )
    .select(
        F.coalesce(F.col("tariff"), F.lit("ALL")).alias("tariff"),
        "reading_quality_count",
        "is_total_tariff",
        "total_commissioned_on_cume_dist",
        "distinct_ids",
        "first_commissioned_on",
    )
    .orderBy(
        F.col("tariff").asc_nulls_last(),
        F.col("reading_quality_count").asc_nulls_last(),
    )
)
