from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_commissioned_on_month = Window.partitionBy(F.date_trunc("month", "commissioned_on"))
w_installed_kw_tier_zone_by_in = Window.partitionBy("installed_kw_tier", "zone").orderBy(F.col("installed_kw").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_installed_kw_tier_zone_by_in_2 = Window.partitionBy("installed_kw_tier", "zone").orderBy(F.col("installed_kw").desc())

scoped_sites = (
    df_sites
    .filter((F.col("zone").isNotNull()) & (F.col("commissioned_on") >= F.lit("2024-08-24").cast("date")))
    .select(
        "id",
        "zone",
        "installed_kw",
        "commissioned_on",
        F.when(F.col("installed_kw") >= 284.0, "large").when(F.col("installed_kw") >= 96.9, "medium").otherwise("small").alias("installed_kw_tier"),
        F.substring(F.trim(F.col("zone")), 1, 4).alias("zone_prefix"),
    )
)

windowed = (
    scoped_sites
    .select(
        "id",
        "zone",
        "installed_kw",
        "commissioned_on",
        "installed_kw_tier",
        F.round(F.col("installed_kw") / F.nullif(F.sum("installed_kw").over(w_commissioned_on_month), F.lit(0)), 4).alias("installed_kw_share_of_commissioned_on_month"),
        F.nth_value("installed_kw", 2).over(w_installed_kw_tier_zone_by_in).alias("second_installed_kw"),
        F.min("installed_kw").over(w_commissioned_on_month).alias("commissioned_on_month_min_installed_kw"),
        F.round(F.cume_dist().over(w_installed_kw_tier_zone_by_in_2), 3).alias("installed_kw_cume_dist"),
        F.first("commissioned_on").over(w_installed_kw_tier_zone_by_in).alias("first_commissioned_on"),
        F.size(F.collect_set("installed_kw_tier").over(w_commissioned_on_month)).alias("distinct_installed_kw_tier_in_commissioned_on_mo"),
    )
)

filtered = (
    windowed.alias("s")
    .join(df_meter_readings.alias("m"), (F.col("m.site_id") == F.col("s.id")) & (F.col("m.voltage") < 235.8) & (F.col("m.quality") == "BAD"), "left_anti")
)

result = (
    filtered
    .select(
        "id",
        "zone",
        "installed_kw",
        "commissioned_on",
        "second_installed_kw",
        "commissioned_on_month_min_installed_kw",
        "installed_kw_cume_dist",
        "first_commissioned_on",
        "distinct_installed_kw_tier_in_commissioned_on_mo",
    )
    .orderBy("id", F.col("second_installed_kw").desc())
)
