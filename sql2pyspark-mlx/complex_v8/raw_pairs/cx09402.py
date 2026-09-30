from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_quality_by_site_installed_kw = Window.partitionBy("quality").orderBy(F.col("site_installed_kw").desc_nulls_first(), "id")
w_quality_site_tariff = Window.partitionBy("quality", "site_tariff")
w_read_at_year_site_id = Window.partitionBy(F.year("read_at"), "site_id")
w_read_at_month_by_site_instal = Window.partitionBy(F.date_trunc("month", "read_at")).orderBy(F.col("site_installed_kw").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_read_at_month_by_site_instal_2 = Window.partitionBy(F.date_trunc("month", "read_at")).orderBy(F.col("site_installed_kw").desc(), "id")
w_site_id_site_tariff_by_site = Window.partitionBy("site_id", "site_tariff").orderBy(F.col("site_installed_kw").desc(), "id")
w_site_id_quality_by_site_inst = Window.partitionBy("site_id", "quality").orderBy(F.col("site_installed_kw").desc(), "id").rowsBetween(-2, Window.currentRow)
w_site_id = Window.partitionBy("site_id")

valid_readings = (
    df_meter_readings
    .select(
        "id",
        "site_id",
        "read_at",
        "voltage",
        "quality",
        F.date_format(F.col("read_at"), "yyyy-MM-dd").alias("read_at_label"),
    )
)

with_parent_site = (
    valid_readings.alias("s")
    .join(df_sites.alias("si"), F.col("si.id") == F.col("s.site_id"), "inner")
    .filter(F.col("si.tariff") == "TOU")
    .select(
        "s.id",
        "s.site_id",
        "s.read_at",
        "s.voltage",
        "s.quality",
        "s.read_at_label",
        F.col("si.tariff").alias("site_tariff"),
        F.col("si.installed_kw").alias("site_installed_kw"),
    )
)

ranked = (
    with_parent_site
    .select(
        "id",
        "read_at",
        "voltage",
        "site_tariff",
        F.ntile(4).over(w_quality_by_site_installed_kw).alias("site_installed_kw_quartile"),
        F.max("voltage").over(w_quality_site_tariff).alias("quality_site_tariff_max_voltage"),
        F.round(F.col("site_installed_kw") - F.avg("site_installed_kw").over(w_quality_site_tariff), 2).alias("site_installed_kw_vs_quality_site_tariff_avg"),
        F.round(F.col("site_installed_kw") / F.nullif(F.sum("site_installed_kw").over(w_read_at_year_site_id), F.lit(0)), 4).alias("site_installed_kw_share_of_read_at_year_site_id"),
        F.sum("voltage").over(w_read_at_month_by_site_instal).alias("running_voltage"),
        F.lag("read_at").over(w_read_at_month_by_site_instal_2).alias("prev_read_at"),
        F.max("site_installed_kw").over(w_read_at_year_site_id).alias("read_at_year_site_id_max_site_installed_kw"),
        F.row_number().over(w_site_id_site_tariff_by_site).alias("seq_no"),
        F.round(F.avg("voltage").over(w_site_id_quality_by_site_inst), 2).alias("voltage_avg_3"),
        F.round(F.col("voltage") / F.nullif(F.sum("voltage").over(w_site_id), F.lit(0)), 4).alias("voltage_share_of_site_id"),
    )
)

tagged = (
    ranked
    .select(
        "*",
        F.concat(F.col("site_tariff"), F.lit("#"), F.col("seq_no").cast("string")).alias("site_tariff_ref"),
        F.abs(F.col("voltage_avg_3") - 227.5).alias("voltage_avg_3_distance"),
    )
)

result = (
    tagged
    .filter(F.col("site_installed_kw_share_of_read_at_year_site_id") > 0.2)
    .select(
        "id",
        "read_at",
        "voltage",
        "site_installed_kw_quartile",
        "site_installed_kw_vs_quality_site_tariff_avg",
        "running_voltage",
        "prev_read_at",
        "read_at_year_site_id_max_site_installed_kw",
        "seq_no",
        "voltage_avg_3",
        "site_tariff_ref",
        "voltage_avg_3_distance",
        F.when(F.col("site_tariff").isin("FLAT", "EV"), "group_a").when(F.col("site_tariff") == "TOU", "group_b").otherwise("other").alias("site_tariff_group"),
        F.substring(F.trim(F.col("site_tariff_ref")), 1, 2).alias("site_tariff_ref_prefix"),
    )
    .orderBy(F.col("read_at").desc(), "site_installed_kw_quartile")
)
