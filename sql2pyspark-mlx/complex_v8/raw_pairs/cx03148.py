from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_zone_zone_len = Window.partitionBy("zone", "zone_len")
w_zone_len_zone_by_installed_k = Window.partitionBy("zone_len", "zone").orderBy(F.col("installed_kw_distance").desc(), "id")
w_zone_len_zone_by_installed_k_2 = Window.partitionBy("zone_len", "zone").orderBy(F.col("installed_kw_distance").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_reading_count = Window.partitionBy("reading_count")

cleaned_sites = (
    df_sites
    .select(
        "id",
        "site_name",
        "zone",
        "tariff",
        "installed_kw",
        F.abs(F.col("installed_kw") - 283.9).alias("installed_kw_distance"),
        F.length(F.trim(F.col("zone"))).alias("zone_len"),
    )
)

with_trends = (
    cleaned_sites
    .select(
        "*",
        F.size(F.collect_set("tariff").over(w_zone_zone_len)).alias("distinct_tariff_in_zone_zone_len"),
        (F.col("zone_len") - F.lag("zone_len").over(w_zone_len_zone_by_installed_k)).alias("zone_len_change"),
        F.lag("zone_len").over(w_zone_len_zone_by_installed_k).alias("prev_zone_len"),
        F.nth_value("installed_kw", 2).over(w_zone_len_zone_by_installed_k_2).alias("second_installed_kw"),
    )
)

reading_stats = (
    df_meter_readings
    .groupBy("site_id")
    .agg(F.count("*").alias("reading_count"))
)

profiled = (
    with_trends.alias("s")
    .join(reading_stats.alias("m"), F.col("m.site_id") == F.col("s.id"), "left")
    .select(
        "s.*",
        F.coalesce(F.col("m.reading_count"), F.lit(0)).alias("reading_count"),
    )
)

scored = (
    profiled
    .select(
        "*",
        F.when(F.col("zone").isin("Z4", "Z2"), "group_a").when(F.col("zone") == "Z3", "group_b").otherwise("other").alias("zone_group"),
        F.abs(F.col("zone_len_change") - -4).alias("zone_len_change_distance"),
        F.round(F.col("installed_kw") / F.nullif(F.col("zone_len"), F.lit(0)), 3).alias("installed_kw_per_zone_len"),
    )
)

ranked = (
    scored
    .withColumn("installed_kw_vs_reading_count_avg", F.round(F.col("installed_kw") - F.avg("installed_kw").over(w_reading_count), 2))
    .withColumn("zone_len_change_vs_reading_count_avg", F.round(F.col("zone_len_change") - F.avg("zone_len_change").over(w_reading_count), 2))
)

result = (
    ranked
    .select(
        "id",
        "site_name",
        "zone",
        "tariff",
        "zone_len",
        "distinct_tariff_in_zone_zone_len",
        "prev_zone_len",
        "second_installed_kw",
        "zone_len_change_distance",
        "installed_kw_per_zone_len",
        "installed_kw_vs_reading_count_avg",
    )
    .orderBy("site_name", F.col("installed_kw_per_zone_len").desc())
)
