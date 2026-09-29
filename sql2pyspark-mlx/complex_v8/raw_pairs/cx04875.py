from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_site_name_len_commissioned_o = Window.partitionBy("site_name_len", F.year("commissioned_on")).orderBy(F.col("installed_kw_distance_minus_installed_kw").desc(), "id")
w_all_by_installed_kw_distance = Window.orderBy(F.col("installed_kw_distance_minus_installed_kw").desc_nulls_first(), "id")
w_all_by_installed_kw_distance_2 = Window.orderBy(F.col("installed_kw_distance_minus_installed_kw").desc_nulls_first(), "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_site_name_len_commissioned_o_2 = Window.partitionBy("site_name_len", F.year("commissioned_on")).orderBy(F.col("installed_kw_distance_minus_installed_kw").desc(), "id").rowsBetween(-2, Window.currentRow)
w_site_name_len_commissioned_o_3 = Window.partitionBy("site_name_len", F.year("commissioned_on")).orderBy(F.col("installed_kw_distance_minus_installed_kw").desc())
w_all_by_first_site_name_len_r = Window.orderBy(F.col("first_site_name_len").desc(), "id", "reading_id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_prev3_commissioned_on_year_b = Window.partitionBy(F.year("prev3_commissioned_on")).orderBy("site_name_len_per_installed_kw", "id", "reading_id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

scoped_sites = (
    df_sites
    .filter((F.upper(F.trim(F.col("site_name"))).like("H%")) | (F.col("zone") == "Z2"))
    .select(
        "id",
        "site_name",
        "tariff",
        "installed_kw",
        "commissioned_on",
        F.coalesce(F.col("installed_kw"), F.lit(0.0)).alias("installed_kw_filled"),
        F.abs(F.col("installed_kw") - 141.6).alias("installed_kw_distance"),
        F.length(F.trim(F.col("site_name"))).alias("site_name_len"),
    )
)

annotated = (
    scoped_sites
    .select(
        "id",
        "site_name",
        "installed_kw",
        "commissioned_on",
        "installed_kw_distance",
        "site_name_len",
        (F.col("installed_kw_filled") + F.col("installed_kw")).alias("installed_kw_filled_plus_installed_kw"),
        (F.col("installed_kw_distance") - F.col("installed_kw")).alias("installed_kw_distance_minus_installed_kw"),
        F.abs(F.col("installed_kw_filled") - 203.0).alias("installed_kw_filled_distance"),
    )
)

derived = (
    annotated
    .filter(F.col("installed_kw_filled_plus_installed_kw") <= 651.8)
    .select(
        "id",
        "site_name",
        "commissioned_on",
        "site_name_len",
        "installed_kw_distance_minus_installed_kw",
        F.round(F.col("site_name_len") / F.nullif(F.col("installed_kw"), F.lit(0)), 3).alias("site_name_len_per_installed_kw"),
        F.date_format(F.col("commissioned_on"), "MMM yyyy").alias("commissioned_on_label"),
    )
)

sequenced = (
    derived
    .select(
        "id",
        "commissioned_on",
        "site_name_len",
        "site_name_len_per_installed_kw",
        "commissioned_on_label",
        F.lag("commissioned_on", 3).over(w_site_name_len_commissioned_o).alias("prev3_commissioned_on"),
        F.dense_rank().over(w_all_by_installed_kw_distance).alias("installed_kw_distance_minus_installed_kw_dense_r"),
        F.first("site_name_len").over(w_all_by_installed_kw_distance_2).alias("first_site_name_len"),
        F.sum("site_name_len").over(w_site_name_len_commissioned_o_2).alias("site_name_len_sum_3"),
        F.round(F.percent_rank().over(w_site_name_len_commissioned_o_3), 3).alias("installed_kw_distance_minus_installed_kw_percent"),
    )
)

filtered = (
    sequenced.alias("s")
    .join(df_meter_readings.alias("m"), F.col("m.site_id") == F.col("s.id"), "left_semi")
)

detail_rows_readings = (
    filtered.alias("s")
    .join(df_meter_readings.alias("m"), (F.col("m.site_id") == F.col("s.id")) & (F.col("m.voltage") < 231.7), "inner")
    .select(
        "s.id",
        "s.site_name_len_per_installed_kw",
        "s.prev3_commissioned_on",
        "s.installed_kw_distance_minus_installed_kw_dense_r",
        "s.first_site_name_len",
        "s.installed_kw_distance_minus_installed_kw_percent",
        F.col("m.id").alias("reading_id"),
        F.col("m.voltage").alias("reading_voltage"),
        F.col("m.kwh").alias("reading_kwh"),
    )
)

with_windows = (
    detail_rows_readings
    .select(
        "id",
        "site_name_len_per_installed_kw",
        "prev3_commissioned_on",
        "installed_kw_distance_minus_installed_kw_dense_r",
        "installed_kw_distance_minus_installed_kw_percent",
        "reading_id",
        "reading_voltage",
        "reading_kwh",
        F.sum("first_site_name_len").over(w_all_by_first_site_name_len_r).alias("running_first_site_name_len"),
        F.min("reading_kwh").over(w_prev3_commissioned_on_year_b).alias("min_reading_kwh_so_far"),
    )
)

result = (
    with_windows
    .filter(F.col("installed_kw_distance_minus_installed_kw_dense_r") < 8)
    .select(
        "id",
        "site_name_len_per_installed_kw",
        "installed_kw_distance_minus_installed_kw_dense_r",
        "installed_kw_distance_minus_installed_kw_percent",
        "reading_id",
        "reading_voltage",
        "reading_kwh",
        "running_first_site_name_len",
        "min_reading_kwh_so_far",
        F.last_day(F.col("prev3_commissioned_on")).alias("prev3_commissioned_on_month_end"),
    )
)
