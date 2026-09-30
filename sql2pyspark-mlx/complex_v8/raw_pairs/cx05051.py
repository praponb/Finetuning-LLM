from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_all_by_installed_kw_tariff_f = Window.orderBy("installed_kw_tariff_flat_distance", "tariff", "zone")
w_zone_zone_group_by_installed = Window.partitionBy("zone", "zone_group").orderBy("installed_kw_tariff_flat_distance", "tariff", "zone")
w_all_by_installed_kw_tariff_f_2 = Window.orderBy("installed_kw_tariff_flat_distance")
w_distinct_ids = Window.partitionBy("distinct_ids")
w_dedup = Window.partitionBy("zone_group").orderBy(F.col("installed_kw_tariff_flat").desc(), F.col("tariff").desc(), F.col("zone").desc())

recent_sites = (
    df_sites
    .select(
        "id",
        "site_name",
        "zone",
        "tariff",
        "installed_kw",
        "commissioned_on",
    )
)

totals = (
    recent_sites
    .groupBy("tariff", "zone")
    .agg(
        F.sum(F.when(F.col("zone") == "Z3", F.col("installed_kw")).otherwise(0)).alias("installed_kw_zone_z3"),
        F.countDistinct("id").alias("distinct_ids"),
        F.sum(F.when(F.col("tariff") == "FLAT", F.col("installed_kw")).otherwise(0)).alias("installed_kw_tariff_flat"),
    )
)

derived = (
    totals
    .select(
        "*",
        F.when(F.col("zone").isin("Z3", "Z2"), "group_a").when(F.col("zone") == "Z4", "group_b").otherwise("other").alias("zone_group"),
        F.abs(F.col("installed_kw_tariff_flat") - 1500.0).alias("installed_kw_tariff_flat_distance"),
        F.concat(F.col("zone"), F.lit("#"), F.col("distinct_ids").cast("string")).alias("zone_ref"),
    )
)

ranked = (
    derived
    .select(
        "*",
        F.ntile(4).over(w_all_by_installed_kw_tariff_f).alias("installed_kw_tariff_flat_distance_quartile"),
        (F.col("installed_kw_zone_z3") - F.lag("installed_kw_zone_z3").over(w_zone_zone_group_by_installed)).alias("installed_kw_zone_z3_change"),
        F.round(F.cume_dist().over(w_all_by_installed_kw_tariff_f_2), 3).alias("installed_kw_tariff_flat_distance_cume_dist"),
        F.count(F.lit(1)).over(w_distinct_ids).alias("rows_in_distinct_ids"),
        F.round(F.avg("installed_kw_tariff_flat").over(w_distinct_ids), 2).alias("distinct_ids_avg_installed_kw_tariff_flat"),
    )
)

zone_group_stats = (
    ranked
    .groupBy("zone_group")
    .agg(F.percentile_approx("rows_in_distinct_ids", 0.5).alias("median_rows_in_distinct_ids"))
)

vs_group = (
    ranked.alias("s")
    .join(zone_group_stats.alias("g"), F.col("g.zone_group") == F.col("s.zone_group"), "inner")
    .filter(F.col("s.rows_in_distinct_ids") >= F.col("g.median_rows_in_distinct_ids"))
    .select(
        "s.*",
        "g.median_rows_in_distinct_ids",
        F.round(F.col("s.rows_in_distinct_ids") - F.col("g.median_rows_in_distinct_ids"), 2).alias("rows_in_distinct_ids_minus_median"),
    )
)

latest = (
    vs_group
    .withColumn("recency_rank", F.row_number().over(w_dedup))
    .filter(F.col("recency_rank") == 1)
    .drop("recency_rank")
)

result = (
    latest
    .select(
        "tariff",
        "installed_kw_zone_z3",
        "distinct_ids",
        "installed_kw_tariff_flat",
        "zone_group",
        "installed_kw_tariff_flat_distance",
        "zone_ref",
        "installed_kw_tariff_flat_distance_quartile",
        "installed_kw_tariff_flat_distance_cume_dist",
        "median_rows_in_distinct_ids",
        F.round(F.col("rows_in_distinct_ids") / F.nullif(F.col("distinct_ids"), F.lit(0)), 3).alias("rows_in_distinct_ids_per_distinct_ids"),
    )
    .orderBy(F.col("zone_ref").desc())
)
