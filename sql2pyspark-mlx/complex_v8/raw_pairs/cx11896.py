from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_commissioned_on_month_tariff = Window.partitionBy(F.date_trunc("month", "commissioned_on"), "tariff")
w_all_by_commissioned_on_ties = Window.orderBy("commissioned_on")
w_tariff_by_commissioned_on_fu = Window.partitionBy("tariff").orderBy(F.col("commissioned_on").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_tariff = Window.partitionBy("tariff")
w_all_by_commissioned_on = Window.orderBy("commissioned_on", "id")
w_tariff_by_installed_kw_full = Window.partitionBy("tariff").orderBy("installed_kw", "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)

scoped = (
    df_sites
    .select(
        "id",
        "site_name",
        "tariff",
        "installed_kw",
        "commissioned_on",
        F.substring(F.trim(F.col("zone")), 1, 3).alias("zone_prefix"),
        (F.floor(F.col("installed_kw") / 100) * 100).alias("installed_kw_bucket"),
    )
)

ranked = (
    scoped
    .select(
        "id",
        "site_name",
        "tariff",
        "installed_kw",
        "commissioned_on",
        "zone_prefix",
        F.count(F.lit(1)).over(w_commissioned_on_month_tariff).alias("rows_in_commissioned_on_month_tariff"),
        F.round(F.percent_rank().over(w_all_by_commissioned_on_ties), 3).alias("commissioned_on_percent_rank"),
        F.min("installed_kw_bucket").over(w_commissioned_on_month_tariff).alias("commissioned_on_month_tariff_min_installed_kw_bu"),
        F.round(F.cume_dist().over(w_all_by_commissioned_on_ties), 3).alias("commissioned_on_cume_dist"),
        F.first("commissioned_on").over(w_tariff_by_commissioned_on_fu).alias("first_commissioned_on"),
        F.max("installed_kw_bucket").over(w_tariff).alias("tariff_max_installed_kw_bucket"),
        F.lead("installed_kw").over(w_all_by_commissioned_on).alias("next_installed_kw"),
        F.round(F.col("installed_kw") - F.avg("installed_kw").over(w_tariff), 2).alias("installed_kw_vs_tariff_avg"),
        F.lead("installed_kw_bucket", 1, 0).over(w_all_by_commissioned_on).alias("next_installed_kw_bucket"),
        F.round(F.col("installed_kw_bucket") / F.nullif(F.sum("installed_kw_bucket").over(w_commissioned_on_month_tariff), F.lit(0)), 4).alias("installed_kw_bucket_share_of_commissioned_on_mon"),
        F.nth_value("commissioned_on", 2).over(w_tariff_by_installed_kw_full).alias("second_commissioned_on"),
    )
)

enriched = (
    ranked
    .filter(F.col("second_commissioned_on") >= F.lit("2024-06-27").cast("date"))
    .select(
        "id",
        "tariff",
        "commissioned_on",
        "zone_prefix",
        "rows_in_commissioned_on_month_tariff",
        "commissioned_on_month_tariff_min_installed_kw_bu",
        "commissioned_on_cume_dist",
        "first_commissioned_on",
        "tariff_max_installed_kw_bucket",
        "next_installed_kw",
        "installed_kw_vs_tariff_avg",
        "installed_kw_bucket_share_of_commissioned_on_mon",
        "second_commissioned_on",
        F.when(F.col("tariff").isin("EV", "TOU"), "group_a").when(F.col("tariff") == "FLAT", "group_b").otherwise("other").alias("tariff_group"),
        F.lower(F.regexp_replace(F.trim(F.col("zone_prefix")), " ", "_")).alias("zone_prefix_slug"),
        F.date_trunc("quarter", F.col("commissioned_on")).alias("commissioned_on_quarter"),
    )
)

result = (
    enriched
    .select(
        "id",
        "commissioned_on",
        "zone_prefix",
        "rows_in_commissioned_on_month_tariff",
        "commissioned_on_month_tariff_min_installed_kw_bu",
        "first_commissioned_on",
        "next_installed_kw",
        "installed_kw_vs_tariff_avg",
        "second_commissioned_on",
        "tariff_group",
        "zone_prefix_slug",
        "commissioned_on_quarter",
        (F.floor(F.col("next_installed_kw") / 100) * 100).alias("next_installed_kw_bucket"),
        (F.floor(F.col("next_installed_kw") / 100) * 100).alias("next_installed_kw_bucket_2"),
    )
    .orderBy(F.col("id").desc(), F.col("tariff_group").desc())
    .limit(10)
)
