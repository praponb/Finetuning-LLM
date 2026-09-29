from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_site_tariff = Window.partitionBy("site_tariff")
w_all_by_read_at_year = Window.orderBy("read_at_year", "id")
w_site_tariff_by_read_at_year = Window.partitionBy("site_tariff").orderBy("read_at_year", "id")
w_site_tariff_by_read_at_year_2 = Window.partitionBy("site_tariff").orderBy("read_at_year", "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_quality_site_id_by_kwh_full = Window.partitionBy("quality", "site_id").orderBy(F.col("kwh").asc_nulls_last(), "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_all_by_read_at_year_ties = Window.orderBy("read_at_year")
w_quality_site_id_by_kwh = Window.partitionBy("quality", "site_id").orderBy(F.col("kwh").asc_nulls_last(), "id")
w_quality_site_id_by_kwh_runni = Window.partitionBy("quality", "site_id").orderBy(F.col("kwh").asc_nulls_last(), "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_quality_site_id_by_kwh_ties = Window.partitionBy("quality", "site_id").orderBy(F.col("kwh").asc_nulls_last())
w_read_at_year_site_tariff_by = Window.partitionBy("read_at_year", "site_tariff").orderBy(F.col("kwh_cume_dist").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_quality_prev_read_at_year = Window.partitionBy("quality", "prev_read_at_year")
w_read_at_year_site_tariff_by_2 = Window.partitionBy("read_at_year", "site_tariff").orderBy(F.col("kwh_cume_dist").desc())

cleaned = (
    df_meter_readings
    .filter((F.col("voltage") <= 232.0) & (F.col("kwh") >= 52.4))
    .select(
        "id",
        "site_id",
        "read_at",
        "kwh",
        "voltage",
        "quality",
        F.year(F.col("read_at")).alias("read_at_year"),
    )
)

combined_details_site = (
    cleaned.alias("s")
    .join(df_sites.alias("si"), F.col("si.id") == F.col("s.site_id"), "inner")
    .select(
        "s.id",
        "s.site_id",
        "s.read_at",
        "s.kwh",
        "s.voltage",
        "s.quality",
        "s.read_at_year",
        F.col("si.commissioned_on").alias("site_commissioned_on"),
        F.col("si.tariff").alias("site_tariff"),
        F.col("si.installed_kw").alias("site_installed_kw"),
    )
)

sequenced = (
    combined_details_site
    .select(
        "*",
        F.size(F.collect_set("quality").over(w_site_tariff)).alias("distinct_quality_in_site_tariff"),
        (F.col("kwh") - F.lag("kwh").over(w_all_by_read_at_year)).alias("kwh_change"),
        F.round(F.col("voltage") - F.avg("voltage").over(w_site_tariff), 2).alias("voltage_vs_site_tariff_avg"),
        F.lag("read_at_year").over(w_all_by_read_at_year).alias("prev_read_at_year"),
        F.round(F.cume_dist().over(w_site_tariff_by_read_at_year), 3).alias("read_at_year_cume_dist"),
        F.lag("voltage", 3).over(w_all_by_read_at_year).alias("prev3_voltage"),
        F.nth_value("read_at", 2).over(w_site_tariff_by_read_at_year_2).alias("second_read_at"),
        F.dense_rank().over(w_site_tariff_by_read_at_year).alias("read_at_year_dense_rank"),
        F.nth_value("kwh", 2).over(w_quality_site_id_by_kwh_full).alias("second_kwh"),
        F.round(F.percent_rank().over(w_all_by_read_at_year), 3).alias("read_at_year_percent_rank"),
        F.round(F.cume_dist().over(w_all_by_read_at_year_ties), 3).alias("read_at_year_cume_dist_2"),
        F.rank().over(w_quality_site_id_by_kwh).alias("kwh_rank"),
        F.round(F.col("read_at_year") - F.avg("read_at_year").over(w_site_tariff), 2).alias("read_at_year_vs_site_tariff_avg"),
        F.max("voltage").over(w_quality_site_id_by_kwh_runni).alias("max_voltage_so_far"),
        F.round(F.cume_dist().over(w_quality_site_id_by_kwh_ties), 3).alias("kwh_cume_dist"),
        F.lead("read_at_year").over(w_quality_site_id_by_kwh).alias("next_read_at_year"),
    )
)

top_rows = (
    sequenced
    .filter(F.col("kwh_rank") == 1)
    .drop("kwh_rank")
)

sequenced_2 = (
    top_rows
    .select(
        "id",
        "site_id",
        "read_at",
        "kwh",
        "voltage",
        "read_at_year",
        "site_installed_kw",
        "kwh_change",
        "voltage_vs_site_tariff_avg",
        "prev_read_at_year",
        "read_at_year_cume_dist",
        "second_read_at",
        "read_at_year_dense_rank",
        "second_kwh",
        "read_at_year_percent_rank",
        "read_at_year_vs_site_tariff_avg",
        "max_voltage_so_far",
        "kwh_cume_dist",
        F.sum("prev_read_at_year").over(w_read_at_year_site_tariff_by).alias("running_prev_read_at_year"),
        F.round(F.col("kwh_change") / F.nullif(F.sum("kwh_change").over(w_quality_prev_read_at_year), F.lit(0)), 4).alias("kwh_change_share_of_quality_prev_read_at_year"),
        F.rank().over(w_read_at_year_site_tariff_by_2).alias("kwh_cume_dist_rank"),
        F.round(F.col("read_at_year") - F.avg("read_at_year").over(w_quality_prev_read_at_year), 2).alias("read_at_year_vs_quality_prev_read_at_year_avg"),
    )
)

result = (
    sequenced_2
    .select(
        "id",
        "site_id",
        "read_at",
        "kwh",
        "read_at_year",
        "kwh_change",
        "voltage_vs_site_tariff_avg",
        "read_at_year_cume_dist",
        "second_read_at",
        "read_at_year_dense_rank",
        "second_kwh",
        "read_at_year_percent_rank",
        "read_at_year_vs_site_tariff_avg",
        "max_voltage_so_far",
        "kwh_cume_dist",
        "kwh_change_share_of_quality_prev_read_at_year",
        "kwh_cume_dist_rank",
    )
)
