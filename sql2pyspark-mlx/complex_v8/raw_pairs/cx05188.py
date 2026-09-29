from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_zone_installed_kw_tier_by_in = Window.partitionBy("zone", "installed_kw_tier").orderBy("installed_kw", "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_installed_kw_tier = Window.partitionBy("installed_kw_tier")
w_installed_kw_tier_by_install = Window.partitionBy("installed_kw_tier").orderBy("installed_kw", "id")
w_zone_installed_kw_tier_by_in_2 = Window.partitionBy("zone", "installed_kw_tier").orderBy("installed_kw", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

base = (
    df_sites
    .filter((F.col("installed_kw") >= 96.9) & (F.col("tariff").isin("TOU", "EV")))
    .select(
        "id",
        "site_name",
        "zone",
        "tariff",
        "installed_kw",
    )
)

annotated = (
    base
    .filter(~F.col("zone").isin("Z3", "Z4"))
    .select(
        "*",
        F.initcap(F.trim(F.col("tariff"))).alias("tariff_clean"),
        F.concat(F.col("site_name"), F.lit("#"), F.col("id").cast("string")).alias("site_name_ref"),
        F.when(F.col("installed_kw") >= 373.3, "HIGH").when(F.col("installed_kw") >= 96.9, "MEDIUM").otherwise("LOW").alias("installed_kw_tier"),
    )
)

annotated_2 = (
    annotated
    .select(
        "*",
        (F.floor(F.col("installed_kw") / 100) * 100).alias("installed_kw_bucket"),
        F.concat_ws("|", F.col("installed_kw_tier"), F.col("tariff_clean")).alias("installed_kw_tier_tariff_clean_label"),
    )
)

with_windows = (
    annotated_2
    .select(
        "id",
        "site_name",
        "zone",
        "installed_kw",
        "site_name_ref",
        "installed_kw_tier",
        "installed_kw_bucket",
        "installed_kw_tier_tariff_clean_label",
        F.nth_value("installed_kw", 2).over(w_zone_installed_kw_tier_by_in).alias("second_installed_kw"),
        F.round(F.avg("installed_kw_bucket").over(w_installed_kw_tier), 2).alias("installed_kw_tier_avg_installed_kw_bucket"),
        F.lag("installed_kw_bucket").over(w_installed_kw_tier_by_install).alias("prev_installed_kw_bucket"),
        F.lead("installed_kw").over(w_installed_kw_tier_by_install).alias("next_installed_kw"),
        F.last("installed_kw_bucket", ignorenulls=True).over(w_zone_installed_kw_tier_by_in_2).alias("last_known_installed_kw_bucket"),
    )
)

reading_stats = (
    df_meter_readings
    .groupBy("site_id")
    .agg(
        F.min("voltage").alias("min_reading_voltage"),
        F.count("*").alias("reading_count"),
    )
)

summarized_rows = (
    with_windows.alias("s")
    .join(reading_stats.alias("m"), F.col("m.site_id") == F.col("s.id"), "left")
    .select(
        "s.*",
        "m.min_reading_voltage",
        F.coalesce(F.col("m.reading_count"), F.lit(0)).alias("reading_count"),
    )
)

totals = (
    summarized_rows
    .filter(F.col("installed_kw_tier_tariff_clean_label").isNotNull())
    .groupBy("reading_count", "zone")
    .agg(
        F.round(F.sum("next_installed_kw") / F.nullif(F.sum("installed_kw_tier_avg_installed_kw_bucket"), F.lit(0)), 3).alias("next_installed_kw_to_installed_kw_tier_avg_insta"),
        F.array_sort(F.collect_set("zone")).alias("zone_list"),
        F.round(F.sum("next_installed_kw"), 2).alias("total_next_installed_kw"),
        F.array_sort(F.collect_set("site_name")).alias("site_name_list"),
        F.min("prev_installed_kw_bucket").alias("min_prev_installed_kw_bucket"),
    )
)

per_label = (
    totals
    .select(
        "reading_count",
        "zone",
        "next_installed_kw_to_installed_kw_tier_avg_insta",
        "zone_list",
        "total_next_installed_kw",
        "min_prev_installed_kw_bucket",
        F.explode("site_name_list").alias("site_name_list_item"),
    )
)

result = (
    per_label
    .filter(F.col("total_next_installed_kw").between(604.0, 2401.0))
    .select(
        "reading_count",
        "zone",
        "next_installed_kw_to_installed_kw_tier_avg_insta",
        "zone_list",
        "total_next_installed_kw",
        "min_prev_installed_kw_bucket",
        F.array_join(F.array_sort(F.col("zone_list")), ",").alias("zone_list_text"),
        F.when(F.col("zone").isin("Z4", "Z3"), "group_a").when(F.col("zone") == "Z2", "group_b").otherwise("other").alias("zone_group"),
    )
    .orderBy("zone_list_text", F.col("min_prev_installed_kw_bucket").desc())
)
