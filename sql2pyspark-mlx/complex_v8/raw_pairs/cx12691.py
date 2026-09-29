from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_cnt_n_length_trim_site_name = Window.partitionBy("cnt", "n_length_trim_site_name_4").orderBy("distinct_zones", "zone", "site_name_len")
w_cnt_n_length_trim_site_name_2 = Window.partitionBy("cnt", "n_length_trim_site_name_4").orderBy("distinct_zones", "zone", "site_name_len").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)

base = (
    df_sites
    .filter(F.col("commissioned_on") >= F.lit("2024-09-15").cast("date"))
    .select(
        "id",
        "site_name",
        "zone",
        "tariff",
        "installed_kw",
        "commissioned_on",
        F.length(F.trim(F.col("site_name"))).alias("site_name_len"),
    )
)

per_group = (
    base
    .groupBy("zone", "site_name_len")
    .agg(
        F.countDistinct("zone").alias("distinct_zones"),
        F.count(F.when(F.length(F.trim(F.col("site_name"))) >= 4, 1)).alias("n_length_trim_site_name_4"),
        F.array_sort(F.collect_set("site_name")).alias("site_name_list"),
        F.count("*").alias("cnt"),
        F.sum(F.when(F.col("zone").isin("Z4", "Z2"), F.col("installed_kw")).otherwise(0)).alias("installed_kw_zone_in_z4_z2"),
    )
)

with_windows = (
    per_group
    .select(
        "*",
        F.lag("installed_kw_zone_in_z4_z2", 3).over(w_cnt_n_length_trim_site_name).alias("prev3_installed_kw_zone_in_z4_z2"),
        F.nth_value("installed_kw_zone_in_z4_z2", 2).over(w_cnt_n_length_trim_site_name_2).alias("second_installed_kw_zone_in_z4_z2"),
        F.row_number().over(w_cnt_n_length_trim_site_name).alias("distinct_zones_seq"),
    )
)

labeled = (
    with_windows
    .filter(F.col("installed_kw_zone_in_z4_z2") >= 250.0)
    .select(
        "*",
        F.length(F.trim(F.col("zone"))).alias("zone_len"),
        F.when(F.col("distinct_zones") >= 6, "HIGH").when(F.col("distinct_zones") >= 5, "MEDIUM").otherwise("LOW").alias("distinct_zones_tier"),
        (F.col("distinct_zones") - F.col("cnt")).alias("distinct_zones_minus_cnt"),
    )
)

distinct_zones_tier_stats = (
    labeled
    .groupBy("distinct_zones_tier")
    .agg(F.avg("second_installed_kw_zone_in_z4_z2").alias("avg_second_installed_kw_zone_in_z4_z2"))
)

compared = (
    labeled.alias("s")
    .join(distinct_zones_tier_stats.alias("g"), F.col("g.distinct_zones_tier") == F.col("s.distinct_zones_tier"), "inner")
    .filter(F.col("s.second_installed_kw_zone_in_z4_z2") > F.col("g.avg_second_installed_kw_zone_in_z4_z2"))
    .select(
        "s.*",
        "g.avg_second_installed_kw_zone_in_z4_z2",
        F.round(F.col("s.second_installed_kw_zone_in_z4_z2") - F.col("g.avg_second_installed_kw_zone_in_z4_z2"), 2).alias("second_installed_kw_zone_in_z4_z2_minus_avg"),
    )
)

result = (
    compared
    .select(
        "zone",
        "site_name_len",
        "n_length_trim_site_name_4",
        "site_name_list",
        "cnt",
        "installed_kw_zone_in_z4_z2",
        "distinct_zones_seq",
        "zone_len",
        "distinct_zones_tier",
        "distinct_zones_minus_cnt",
        "avg_second_installed_kw_zone_in_z4_z2",
        "second_installed_kw_zone_in_z4_z2_minus_avg",
    )
    .orderBy(
        "distinct_zones_tier",
        F.col("n_length_trim_site_name_4").asc_nulls_last(),
    )
)
