from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_site_id_by_kwh = Window.partitionBy("site_id").orderBy("kwh", "id")
w_site_id = Window.partitionBy("site_id")
w_site_id_by_site_installed_kw = Window.partitionBy("site_id").orderBy(F.col("site_installed_kw").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_all_by_site_installed_kw_tie = Window.orderBy("site_installed_kw")
w_all_by_site_installed_kw = Window.orderBy("site_installed_kw", "id")
w_site_id_by_voltage_full = Window.partitionBy("site_id").orderBy(F.col("voltage").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_site_id_by_voltage_running = Window.partitionBy("site_id").orderBy(F.col("voltage").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

cleaned = (
    df_meter_readings
    .select(
        "id",
        "site_id",
        "kwh",
        "voltage",
        "quality",
    )
)

combined_details_site = (
    cleaned.alias("s")
    .join(df_sites.alias("si"), (F.col("si.id") == F.col("s.site_id")) & (F.upper(F.trim(F.col("si.site_name"))).like("S%")), "inner")
    .select(
        "s.id",
        "s.site_id",
        "s.kwh",
        "s.voltage",
        F.col("si.installed_kw").alias("site_installed_kw"),
        "si.site_name",
    )
)

sequenced = (
    combined_details_site
    .select(
        "*",
        F.lead("kwh").over(w_site_id_by_kwh).alias("next_kwh"),
        F.count(F.lit(1)).over(w_site_id).alias("rows_in_site_id"),
        F.last("site_installed_kw").over(w_site_id_by_site_installed_kw).alias("last_site_installed_kw"),
        F.rank().over(w_all_by_site_installed_kw_tie).alias("site_installed_kw_rank"),
        F.sum("kwh").over(w_site_id).alias("site_id_sum_kwh"),
        F.lead("voltage").over(w_all_by_site_installed_kw).alias("next_voltage"),
        F.lag("kwh", 1, 0).over(w_all_by_site_installed_kw).alias("prev_kwh"),
        F.first("voltage").over(w_site_id_by_voltage_full).alias("first_voltage"),
        (F.col("kwh") - F.lag("kwh").over(w_all_by_site_installed_kw)).alias("kwh_change"),
        F.lead("kwh", 3).over(w_site_id_by_kwh).alias("next3_kwh"),
        F.first("voltage", ignorenulls=True).over(w_site_id_by_voltage_running).alias("first_known_voltage"),
        F.lag("voltage").over(w_all_by_site_installed_kw).alias("prev_voltage"),
    )
)

result = (
    sequenced
    .select(
        "id",
        "site_id",
        "kwh",
        "voltage",
        "site_installed_kw",
        "site_name",
        "next_kwh",
        "rows_in_site_id",
        "last_site_installed_kw",
        "site_installed_kw_rank",
        "site_id_sum_kwh",
        "next_voltage",
        "prev_kwh",
        "first_voltage",
        "kwh_change",
        "next3_kwh",
        "first_known_voltage",
        "prev_voltage",
        F.concat(F.col("site_name"), F.lit("#"), F.col("rows_in_site_id").cast("string")).alias("site_name_ref"),
        F.coalesce(F.col("site_id_sum_kwh"), F.lit(0.0)).alias("site_id_sum_kwh_filled"),
    )
    .orderBy(F.col("rows_in_site_id").desc(), "site_name_ref")
)
