from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_all_by_kwh_moving_2 = Window.orderBy(F.col("kwh").desc_nulls_first(), "id").rowsBetween(-2, Window.currentRow)
w_site_id_by_site_installed_kw = Window.partitionBy("site_id").orderBy("site_installed_kw", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_site_zone_site_id = Window.partitionBy("site_zone", "site_id")
w_site_id = Window.partitionBy("site_id")
w_all_by_kwh_running = Window.orderBy(F.col("kwh").desc_nulls_first(), "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_all_by_site_installed_kw = Window.orderBy(F.col("site_installed_kw").desc(), "id")
w_all_by_kwh_adj_avg_3_running = Window.orderBy(F.col("kwh_adj_avg_3").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_quality_distinct_quality_in = Window.partitionBy("quality", "distinct_quality_in_site_zone_site_id").orderBy("site_installed_kw_bucket")
w_site_zone_distinct_quality_i = Window.partitionBy("site_zone", "distinct_quality_in_site_id")

filtered = (
    df_meter_readings
    .filter(F.col("quality").isin("EST", "OK"))
    .select(
        "id",
        "site_id",
        "read_at",
        "kwh",
        "voltage",
        "quality",
        F.coalesce(F.col("kwh"), F.lit(0.0)).alias("kwh_filled"),
        F.round(F.col("kwh") * 1.07, 2).alias("kwh_adj"),
    )
)

combined_details_site = (
    filtered.alias("s")
    .join(df_sites.alias("si"), F.col("si.id") == F.col("s.site_id"), "inner")
    .select(
        "s.id",
        "s.site_id",
        "s.kwh",
        "s.quality",
        "s.kwh_filled",
        "s.kwh_adj",
        F.col("si.zone").alias("site_zone"),
        F.col("si.installed_kw").alias("site_installed_kw"),
    )
)

windowed = (
    combined_details_site
    .select(
        "*",
        F.round(F.avg("kwh_adj").over(w_all_by_kwh_moving_2), 2).alias("kwh_adj_avg_3"),
        F.first("kwh", ignorenulls=True).over(w_site_id_by_site_installed_kw).alias("first_known_kwh"),
        F.round(F.col("kwh_filled") / F.nullif(F.sum("kwh_filled").over(w_site_zone_site_id), F.lit(0)), 4).alias("kwh_filled_share_of_site_zone_site_id"),
        F.size(F.collect_set("quality").over(w_site_zone_site_id)).alias("distinct_quality_in_site_zone_site_id"),
        F.count(F.lit(1)).over(w_site_id).alias("rows_in_site_id"),
        F.sum("kwh_adj").over(w_all_by_kwh_running).alias("running_kwh_adj"),
        F.lag("kwh_filled", 2).over(w_all_by_site_installed_kw).alias("prev2_kwh_filled"),
        F.size(F.collect_set("quality").over(w_site_id)).alias("distinct_quality_in_site_id"),
    )
)

tagged = (
    windowed
    .select(
        "*",
        F.concat_ws("-", F.col("quality"), F.col("site_zone")).alias("quality_site_zone_label"),
        F.coalesce(F.col("site_zone"), F.lit("UNKNOWN")).alias("site_zone_or_unknown"),
        (F.floor(F.col("site_installed_kw") / 100) * 100).alias("site_installed_kw_bucket"),
    )
)

sequenced = (
    tagged
    .select(
        "*",
        F.last("site_installed_kw_bucket", ignorenulls=True).over(w_all_by_kwh_adj_avg_3_running).alias("last_known_site_installed_kw_bucket"),
        F.round(F.cume_dist().over(w_quality_distinct_quality_in), 3).alias("site_installed_kw_bucket_cume_dist"),
        F.round(F.col("kwh") - F.avg("kwh").over(w_site_zone_distinct_quality_i), 2).alias("kwh_vs_site_zone_distinct_quality_in_site_id_avg"),
        F.count(F.lit(1)).over(w_site_zone_distinct_quality_i).alias("rows_in_site_zone_distinct_quality_in_site_id"),
    )
)

result = (
    sequenced
    .select(
        "id",
        "kwh",
        "kwh_filled",
        "kwh_adj",
        "site_installed_kw",
        "kwh_adj_avg_3",
        "first_known_kwh",
        "distinct_quality_in_site_zone_site_id",
        "rows_in_site_id",
        "running_kwh_adj",
        "prev2_kwh_filled",
        "distinct_quality_in_site_id",
        "quality_site_zone_label",
        "site_installed_kw_bucket",
        "last_known_site_installed_kw_bucket",
        "site_installed_kw_bucket_cume_dist",
        "kwh_vs_site_zone_distinct_quality_in_site_id_avg",
        "rows_in_site_zone_distinct_quality_in_site_id",
        F.coalesce(F.col("site_zone"), F.lit("UNKNOWN")).alias("site_zone_or_unknown_2"),
    )
    .orderBy("site_installed_kw", F.col("id").desc_nulls_first())
    .limit(20)
)
