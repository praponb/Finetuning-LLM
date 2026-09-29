from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_site_tariff = Window.partitionBy("site_tariff")
w_quality_by_site_installed_kw = Window.partitionBy("quality").orderBy("site_installed_kw", "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_quality_by_site_installed_kw_2 = Window.partitionBy("quality").orderBy("site_installed_kw", "id").rowsBetween(-6, Window.currentRow)
w_read_at_month = Window.partitionBy(F.month("read_at"))
w_quality_by_site_installed_kw_3 = Window.partitionBy("quality").orderBy("site_installed_kw")
w_read_at_to_date_by_site_inst = Window.partitionBy(F.to_date("read_at")).orderBy("site_installed_kw", "id")
w_read_at_to_date_by_site_inst_2 = Window.partitionBy(F.to_date("read_at")).orderBy("site_installed_kw", "id").rowsBetween(-3, -1)
w_quality_site_id_by_kwh_runni = Window.partitionBy("quality", "site_id").orderBy("kwh", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_all_by_voltage = Window.orderBy(F.col("voltage").desc_nulls_first(), "id")
w_quality_by_site_installed_kw_4 = Window.partitionBy("quality").orderBy("site_installed_kw", "id")
w_quality_by_site_installed_kw_5 = Window.partitionBy("quality").orderBy("site_installed_kw", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_site_id_quality = Window.partitionBy("site_id", "quality")
w_distinct_quality_in_read_at = Window.partitionBy("distinct_quality_in_read_at_month", "quality").orderBy("voltage_avg_3_prior", "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_all_by_site_installed_kw_sum = Window.orderBy("site_installed_kw_sum_7", "id")

scoped = (
    df_meter_readings
    .select(
        "id",
        "site_id",
        "read_at",
        "kwh",
        "voltage",
        "quality",
        F.date_format(F.col("read_at"), "yyyy-MM-dd").alias("read_at_label"),
    )
)

joined_site = (
    scoped.alias("s")
    .join(df_sites.alias("si"), F.col("si.id") == F.col("s.site_id"), "inner")
    .select(
        "s.*",
        F.col("si.tariff").alias("site_tariff"),
        F.col("si.installed_kw").alias("site_installed_kw"),
    )
)

with_windows = (
    joined_site
    .select(
        "*",
        F.round(F.col("voltage") / F.nullif(F.sum("voltage").over(w_site_tariff), F.lit(0)), 4).alias("voltage_share_of_site_tariff"),
        F.first("kwh").over(w_quality_by_site_installed_kw).alias("first_kwh"),
        F.sum("site_installed_kw").over(w_quality_by_site_installed_kw_2).alias("site_installed_kw_sum_7"),
        F.round(F.col("site_installed_kw") - F.avg("site_installed_kw").over(w_read_at_month), 2).alias("site_installed_kw_vs_read_at_month_avg"),
        F.round(F.percent_rank().over(w_quality_by_site_installed_kw_3), 3).alias("site_installed_kw_percent_rank"),
        F.round(F.col("kwh") / F.nullif(F.sum("kwh").over(w_read_at_month), F.lit(0)), 4).alias("kwh_share_of_read_at_month"),
        F.round((F.unix_timestamp("read_at") - F.unix_timestamp(F.lag("read_at").over(w_read_at_to_date_by_site_inst))) / 60, 1).alias("minutes_since_prev_read_at"),
        F.round(F.avg("voltage").over(w_read_at_to_date_by_site_inst_2), 2).alias("voltage_avg_3_prior"),
        F.max("kwh").over(w_quality_site_id_by_kwh_runni).alias("max_kwh_so_far"),
        F.size(F.collect_set("quality").over(w_read_at_month)).alias("distinct_quality_in_read_at_month"),
        F.round(F.col("site_installed_kw") - F.avg("site_installed_kw").over(w_site_tariff), 2).alias("site_installed_kw_vs_site_tariff_avg"),
        F.round(F.avg("site_installed_kw").over(w_site_tariff), 2).alias("site_tariff_avg_site_installed_kw"),
        F.ntile(4).over(w_all_by_voltage).alias("voltage_quartile"),
        F.lead("voltage", 2).over(w_quality_by_site_installed_kw_4).alias("next2_voltage"),
        F.first("kwh", ignorenulls=True).over(w_quality_by_site_installed_kw_5).alias("first_known_kwh"),
    )
)

enriched = (
    with_windows
    .filter(F.col("site_id") <= 4)
    .select(
        "*",
        F.datediff(F.lit("2025-06-30").cast("date"), F.col("read_at")).alias("days_since_read_at"),
        F.round(F.col("voltage") / F.nullif(F.col("kwh"), F.lit(0)), 3).alias("voltage_per_kwh"),
    )
)

ranked = (
    enriched
    .select(
        "*",
        F.round(F.col("kwh") - F.avg("kwh").over(w_site_id_quality), 2).alias("kwh_vs_site_id_quality_avg"),
        F.round(F.avg("next2_voltage").over(w_site_id_quality), 2).alias("site_id_quality_avg_next2_voltage"),
        F.count(F.lit(1)).over(w_site_id_quality).alias("rows_in_site_id_quality"),
        F.first("read_at").over(w_distinct_quality_in_read_at).alias("first_read_at"),
        F.round(F.cume_dist().over(w_all_by_site_installed_kw_sum), 3).alias("site_installed_kw_sum_7_cume_dist"),
    )
)

result = (
    ranked
    .select(
        "id",
        "site_id",
        "read_at",
        "kwh",
        "quality",
        "site_tariff",
        "site_installed_kw",
        "first_kwh",
        "site_installed_kw_vs_read_at_month_avg",
        "minutes_since_prev_read_at",
        "voltage_avg_3_prior",
        "max_kwh_so_far",
        "distinct_quality_in_read_at_month",
        "site_installed_kw_vs_site_tariff_avg",
        "site_tariff_avg_site_installed_kw",
        "voltage_quartile",
        "next2_voltage",
        "first_known_kwh",
        "days_since_read_at",
        "voltage_per_kwh",
        "site_id_quality_avg_next2_voltage",
        "first_read_at",
        "site_installed_kw_sum_7_cume_dist",
        F.when(F.col("quality").isin("BAD", "EST"), "group_a").when(F.col("quality") == "OK", "group_b").otherwise("other").alias("quality_group"),
        F.round(F.col("rows_in_site_id_quality") / F.nullif(F.col("first_known_kwh"), F.lit(0)), 3).alias("rows_in_site_id_quality_per_first_known_kwh"),
    )
    .orderBy(F.col("quality_group").desc_nulls_first())
)
