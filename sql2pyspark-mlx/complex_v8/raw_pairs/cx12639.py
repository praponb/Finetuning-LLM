from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_quality_site_id = Window.partitionBy("quality", "site_id")
w_all_by_kwh_moving_6 = Window.orderBy("kwh", "id").rowsBetween(-6, Window.currentRow)
w_all_by_kwh = Window.orderBy("kwh", "id")
w_all_by_voltage_sum_7_running = Window.orderBy("voltage_sum_7", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_site_id = Window.partitionBy("site_id")
w_all_by_voltage_sum_7_ties = Window.orderBy("voltage_sum_7")

recent_readings = (
    df_meter_readings
    .filter(F.col("kwh") >= 43.1)
    .select(
        "id",
        "site_id",
        "kwh",
        "voltage",
        "quality",
        F.initcap(F.trim(F.col("quality"))).alias("quality_clean"),
    )
)

with_windows = (
    recent_readings
    .select(
        "id",
        "site_id",
        "kwh",
        "voltage",
        "quality",
        F.sum("voltage").over(w_all_by_kwh_moving_6).alias("voltage_sum_7"),
        F.ntile(4).over(w_all_by_kwh).alias("kwh_quartile"),
        F.round(F.col("voltage") / F.nullif(F.sum("voltage").over(w_quality_site_id), F.lit(0)), 4).alias("voltage_share_of_quality_site_id"),
        F.min("kwh").over(w_quality_site_id).alias("quality_site_id_min_kwh"),
    )
)

with_windows_2 = (
    with_windows
    .select(
        "*",
        F.first("kwh_quartile", ignorenulls=True).over(w_all_by_voltage_sum_7_running).alias("first_known_kwh_quartile"),
        F.size(F.collect_set("quality").over(w_site_id)).alias("distinct_quality_in_site_id"),
        F.rank().over(w_all_by_voltage_sum_7_ties).alias("voltage_sum_7_rank"),
        F.count(F.lit(1)).over(w_all_by_voltage_sum_7_running).alias("running_count"),
        F.sum("quality_site_id_min_kwh").over(w_all_by_voltage_sum_7_running).alias("running_quality_site_id_min_kwh"),
    )
)

enriched = (
    with_windows_2.alias("s")
    .join(df_sites.alias("si"), F.col("si.id") == F.col("s.site_id"), "inner")
    .select(
        "s.id",
        "s.site_id",
        "s.quality",
        "s.voltage_sum_7",
        "s.kwh_quartile",
        "s.voltage_share_of_quality_site_id",
        "s.quality_site_id_min_kwh",
        "s.distinct_quality_in_site_id",
        "s.voltage_sum_7_rank",
        "s.running_quality_site_id_min_kwh",
        F.col("si.commissioned_on").alias("site_commissioned_on"),
    )
)

result = (
    enriched
    .filter(F.col("quality") == "OK")
    .select(
        "id",
        "quality",
        "kwh_quartile",
        "voltage_share_of_quality_site_id",
        "voltage_sum_7_rank",
        "running_quality_site_id_min_kwh",
        F.concat(F.col("quality"), F.lit("#"), F.col("kwh_quartile").cast("string")).alias("quality_ref"),
    )
    .orderBy("kwh_quartile")
)
