from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_site_commissioned_on_month_b = Window.partitionBy(F.month("site_commissioned_on")).orderBy(F.col("voltage").desc(), "id")
w_site_commissioned_on_month_b_2 = Window.partitionBy(F.month("site_commissioned_on")).orderBy(F.col("voltage").desc())
w_site_id_site_commissioned_on = Window.partitionBy("site_id", F.year("site_commissioned_on"))
w_site_id = Window.partitionBy("site_id")
w_site_id_quality_by_voltage_m = Window.partitionBy("site_id", "quality").orderBy("voltage", "id").rowsBetween(-2, Window.currentRow)
w_site_id_quality_by_voltage = Window.partitionBy("site_id", "quality").orderBy("voltage", "id")
w_site_commissioned_on_month_s = Window.partitionBy(F.date_trunc("month", "site_commissioned_on"), "site_id").orderBy("site_commissioned_on", "id")
w_rn_quality_by_site_installed = Window.partitionBy("rn", "quality").orderBy("site_installed_kw", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_rn_voltage_dense_rank = Window.partitionBy("rn", "voltage_dense_rank")
w_rn_quality_by_site_installed_2 = Window.partitionBy("rn", "quality").orderBy("site_installed_kw", "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)

base_readings = (
    df_meter_readings
    .filter((F.col("quality") == "EST") & (F.col("kwh") < 111.9))
    .select(
        "id",
        "site_id",
        "kwh",
        "voltage",
        "quality",
    )
)

joined_site = (
    base_readings.alias("s")
    .join(df_sites.alias("si"), F.col("si.id") == F.col("s.site_id"), "left")
    .select(
        "s.id",
        "s.site_id",
        "s.kwh",
        "s.voltage",
        "s.quality",
        F.col("si.installed_kw").alias("site_installed_kw"),
        "si.site_name",
        F.col("si.commissioned_on").alias("site_commissioned_on"),
    )
)

with_windows = (
    joined_site
    .select(
        "*",
        (F.col("kwh") - F.lag("kwh").over(w_site_commissioned_on_month_b)).alias("kwh_change"),
        F.dense_rank().over(w_site_commissioned_on_month_b_2).alias("voltage_dense_rank"),
        F.round(F.col("kwh") - F.avg("kwh").over(w_site_id_site_commissioned_on), 2).alias("kwh_vs_site_id_site_commissioned_on_year_avg"),
        F.min("site_installed_kw").over(w_site_id).alias("site_id_min_site_installed_kw"),
        F.round(F.avg("voltage").over(w_site_id_quality_by_voltage_m), 2).alias("voltage_avg_3"),
        F.size(F.collect_set("quality").over(w_site_id)).alias("distinct_quality_in_site_id"),
        F.round(F.col("kwh") / F.nullif(F.sum("kwh").over(w_site_id), F.lit(0)), 4).alias("kwh_share_of_site_id"),
        (F.col("site_installed_kw") - F.lag("site_installed_kw").over(w_site_id_quality_by_voltage)).alias("site_installed_kw_change"),
        F.row_number().over(w_site_commissioned_on_month_s).alias("rn"),
    )
)

ranked = (
    with_windows
    .select(
        "id",
        "site_id",
        "voltage",
        "site_installed_kw",
        "site_name",
        "site_commissioned_on",
        "voltage_dense_rank",
        "kwh_vs_site_id_site_commissioned_on_year_avg",
        "site_id_min_site_installed_kw",
        "distinct_quality_in_site_id",
        "kwh_share_of_site_id",
        "site_installed_kw_change",
        F.first("voltage_dense_rank", ignorenulls=True).over(w_rn_quality_by_site_installed).alias("first_known_voltage_dense_rank"),
        F.round(F.col("kwh_change") / F.nullif(F.sum("kwh_change").over(w_rn_voltage_dense_rank), F.lit(0)), 4).alias("kwh_change_share_of_rn_voltage_dense_rank"),
        F.first("kwh_share_of_site_id").over(w_rn_quality_by_site_installed_2).alias("first_kwh_share_of_site_id"),
        F.sum("site_installed_kw_change").over(w_rn_quality_by_site_installed).alias("running_site_installed_kw_change"),
    )
)

result = (
    ranked
    .select(
        "id",
        "site_installed_kw",
        "site_name",
        "site_commissioned_on",
        "voltage_dense_rank",
        "site_id_min_site_installed_kw",
        "distinct_quality_in_site_id",
        "site_installed_kw_change",
        "kwh_change_share_of_rn_voltage_dense_rank",
        "first_kwh_share_of_site_id",
        F.date_format(F.col("site_commissioned_on"), "MMM yyyy").alias("site_commissioned_on_label"),
    )
    .orderBy(
        F.col("site_installed_kw_change").asc_nulls_last(),
        F.col("site_commissioned_on").desc(),
    )
)
