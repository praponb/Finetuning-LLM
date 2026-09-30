from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_all_by_distinct_tariff_group = Window.orderBy(F.col("distinct_tariff_groups").desc_nulls_first(), "tariff", "commissioned_on_month")
w_all_by_distinct_zones_full = Window.orderBy(F.col("distinct_zones").desc(), "tariff", "commissioned_on_month").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_all_by_distinct_zones = Window.orderBy(F.col("distinct_zones").desc(), "tariff", "commissioned_on_month")
w_commissioned_on_month_to_dat = Window.partitionBy(F.to_date("commissioned_on_month"))

cleaned = (
    df_sites
    .filter((F.lower(F.col("site_name")).like("% b%")) | (F.col("commissioned_on") < F.lit("2024-11-21").cast("date")))
    .select(
        "id",
        "zone",
        "tariff",
        "installed_kw",
        "commissioned_on",
        F.date_add(F.col("commissioned_on"), 30).alias("commissioned_on_plus_30d"),
        F.round(F.col("installed_kw") * 0.25, 2).alias("installed_kw_adj"),
        F.concat(F.col("site_name"), F.lit("#"), F.col("id").cast("string")).alias("site_name_ref"),
    )
)

qualified = (
    cleaned.alias("s")
    .join(df_meter_readings.alias("m"), (F.col("m.site_id") == F.col("s.id")) & (~F.col("m.quality").isin("OK", "BAD")), "left_semi")
)

annotated = (
    qualified
    .filter(F.col("installed_kw_adj") >= 154.0)
    .select(
        "*",
        F.when(F.col("tariff").isin("TOU", "EV"), "group_a").when(F.col("tariff") == "FLAT", "group_b").otherwise("other").alias("tariff_group"),
        F.concat_ws("-", F.col("zone"), F.col("tariff")).alias("zone_tariff_label"),
    )
)

reading_stats = (
    df_meter_readings
    .groupBy("site_id")
    .agg(F.min("voltage").alias("min_reading_voltage"))
)

summarized_rows = (
    annotated.alias("s")
    .join(reading_stats.alias("m"), F.col("m.site_id") == F.col("s.id"), "left")
    .select("s.*", "m.min_reading_voltage")
)

tariff_group_stats = (
    summarized_rows
    .groupBy("tariff_group")
    .agg(F.percentile_approx("installed_kw_adj", 0.5).alias("median_installed_kw_adj"))
)

above_average = (
    summarized_rows.alias("s")
    .join(tariff_group_stats.alias("g"), F.col("g.tariff_group") == F.col("s.tariff_group"), "inner")
    .filter(F.col("s.installed_kw_adj") >= F.col("g.median_installed_kw_adj"))
    .select(
        "s.*",
        "g.median_installed_kw_adj",
        F.round(F.col("s.installed_kw_adj") - F.col("g.median_installed_kw_adj"), 2).alias("installed_kw_adj_minus_median"),
    )
)

summary = (
    above_average
    .filter(F.col("site_name_ref").isNotNull())
    .groupBy(
        "tariff",
        F.date_trunc("month", "commissioned_on").alias("commissioned_on_month"),
    )
    .agg(
        F.countDistinct("tariff_group").alias("distinct_tariff_groups"),
        F.sum(F.when(F.col("tariff") == "TOU", F.col("installed_kw")).otherwise(0)).alias("installed_kw_tariff_tou"),
        F.countDistinct("zone").alias("distinct_zones"),
        F.count("*").alias("row_count"),
        F.sum("min_reading_voltage").alias("sum_min_reading_voltage_check"),
    )
    .filter(F.col("sum_min_reading_voltage_check") > 215.2)
    .drop("sum_min_reading_voltage_check")
)

with_trends = (
    summary
    .select(
        "tariff",
        "commissioned_on_month",
        "distinct_tariff_groups",
        "installed_kw_tariff_tou",
        "row_count",
        F.dense_rank().over(w_all_by_distinct_tariff_group).alias("distinct_tariff_groups_dense_rank"),
        F.nth_value("installed_kw_tariff_tou", 2).over(w_all_by_distinct_zones_full).alias("second_installed_kw_tariff_tou"),
        F.ntile(4).over(w_all_by_distinct_zones).alias("distinct_zones_quartile"),
        F.count(F.lit(1)).over(w_commissioned_on_month_to_dat).alias("rows_in_commissioned_on_month_to_date"),
    )
)

result = (
    with_trends
    .select(
        "tariff",
        "commissioned_on_month",
        "second_installed_kw_tariff_tou",
        "installed_kw_tariff_tou",
    )
    .unpivot(
        ids=["tariff", "commissioned_on_month"],
        values=["second_installed_kw_tariff_tou", "installed_kw_tariff_tou"],
        variableColumnName="field",
        valueColumnName="reading",
    )
    .filter(F.col("reading").isNotNull())
)
