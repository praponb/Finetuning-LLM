from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_site_id_by_site_installed_kw = Window.partitionBy("site_id").orderBy(F.col("site_installed_kw").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_site_id_by_site_installed_kw_2 = Window.partitionBy("site_id").orderBy(F.col("site_installed_kw").desc(), "id").rowsBetween(-2, -1)
w_all_by_site_installed_kw_ful = Window.orderBy(F.col("site_installed_kw").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_read_at_month_by_voltage = Window.partitionBy(F.date_trunc("month", "read_at")).orderBy(F.col("voltage").desc(), "id")
w_site_id_site_tariff = Window.partitionBy("site_id", "site_tariff")
w_site_id_by_site_installed_kw_3 = Window.partitionBy("site_id").orderBy(F.col("site_installed_kw").desc(), "id")
w_read_at_year_site_tariff = Window.partitionBy(F.year("read_at"), "site_tariff")
w_voltage_rank = Window.partitionBy("voltage_rank")
w_all_by_last_voltage_full = Window.orderBy("last_voltage", "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_read_at_year = Window.partitionBy("read_at_year")
w_all_by_pct_site_tariff_tou_r = Window.orderBy(F.col("pct_site_tariff_tou").desc(), "read_at_year").rowsBetween(Window.unboundedPreceding, Window.currentRow)

base_readings = (
    df_meter_readings
    .filter((F.col("voltage") > 218.4) & (F.col("kwh") >= 50.4))
    .select(
        "id",
        "site_id",
        "read_at",
        "kwh",
        "voltage",
        "quality",
    )
)

enriched = (
    base_readings.alias("s")
    .join(df_sites.alias("si"), F.col("si.id") == F.col("s.site_id"), "inner")
    .select(
        "s.id",
        "s.site_id",
        "s.read_at",
        "s.kwh",
        "s.voltage",
        F.col("si.tariff").alias("site_tariff"),
        F.col("si.installed_kw").alias("site_installed_kw"),
        "si.site_name",
    )
)

sequenced = (
    enriched
    .select(
        "id",
        "read_at",
        "site_tariff",
        "site_installed_kw",
        F.first("site_installed_kw", ignorenulls=True).over(w_site_id_by_site_installed_kw).alias("first_known_site_installed_kw"),
        F.sum("site_installed_kw").over(w_site_id_by_site_installed_kw_2).alias("site_installed_kw_sum_2_prior"),
        F.last("voltage").over(w_all_by_site_installed_kw_ful).alias("last_voltage"),
        F.rank().over(w_read_at_month_by_voltage).alias("voltage_rank"),
        F.round(F.col("voltage") / F.nullif(F.sum("voltage").over(w_site_id_site_tariff), F.lit(0)), 4).alias("voltage_share_of_site_id_site_tariff"),
        F.round(F.col("site_installed_kw") / F.nullif(F.sum("site_installed_kw").over(w_site_id_site_tariff), F.lit(0)), 4).alias("site_installed_kw_share_of_site_id_site_tariff"),
        F.lead("site_installed_kw", 2, 0).over(w_site_id_by_site_installed_kw_3).alias("next2_site_installed_kw"),
        F.round(F.avg("kwh").over(w_read_at_year_site_tariff), 2).alias("read_at_year_site_tariff_avg_kwh"),
        F.round(F.col("voltage") - F.avg("voltage").over(w_read_at_year_site_tariff), 2).alias("voltage_vs_read_at_year_site_tariff_avg"),
    )
)

metrics = (
    sequenced
    .select(
        "*",
        F.count(F.lit(1)).over(w_voltage_rank).alias("rows_in_voltage_rank"),
        F.first("last_voltage").over(w_all_by_last_voltage_full).alias("first_last_voltage"),
        F.round(F.col("site_installed_kw") - F.avg("site_installed_kw").over(w_voltage_rank), 2).alias("site_installed_kw_vs_voltage_rank_avg"),
    )
)

summary = (
    metrics
    .groupBy(F.year("read_at").alias("read_at_year"))
    .agg(
        F.sum("site_installed_kw_vs_voltage_rank_avg").alias("total_site_installed_kw_vs_voltage_rank_avg"),
        F.round(F.avg("site_installed_kw_vs_voltage_rank_avg"), 2).alias("avg_site_installed_kw_vs_voltage_rank_avg"),
        F.round(100 * F.count(F.when(F.col("site_tariff") == "TOU", 1)) / F.count("*"), 1).alias("pct_site_tariff_tou"),
        F.array_sort(F.collect_set("site_tariff")).alias("site_tariff_list"),
    )
)

sequenced_2 = (
    summary
    .select(
        "*",
        F.max("read_at_year").over(w_read_at_year).alias("read_at_year_max_read_at_year"),
        F.max("pct_site_tariff_tou").over(w_all_by_pct_site_tariff_tou_r).alias("max_pct_site_tariff_tou_so_far"),
    )
)

result = (
    sequenced_2
    .filter(F.col("avg_site_installed_kw_vs_voltage_rank_avg") <= 250.0)
    .select(
        "read_at_year",
        "avg_site_installed_kw_vs_voltage_rank_avg",
        "pct_site_tariff_tou",
        "read_at_year_max_read_at_year",
        "max_pct_site_tariff_tou_so_far",
    )
    .orderBy(
        F.col("max_pct_site_tariff_tou_so_far").desc(),
        "read_at_year_max_read_at_year",
    )
)
