from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_site_id_by_site_installed_kw = Window.partitionBy("site_id").orderBy(F.col("site_installed_kw").desc(), "id")
w_site_id_by_voltage_running = Window.partitionBy("site_id").orderBy(F.col("voltage").desc_nulls_first(), "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_site_id = Window.partitionBy("site_id")
w_quality_site_id_by_voltage_d = Window.partitionBy("quality", "site_id").orderBy(F.col("voltage_distance").desc(), "id")
w_site_id_quality_by_site_inst = Window.partitionBy("site_id", "quality").orderBy("site_installed_kw", "id").rowsBetween(-3, Window.currentRow)
w_site_id_n_rows = Window.partitionBy("site_id", "n_rows")

recent_readings = (
    df_meter_readings
    .filter(F.col("quality") != "OK")
    .select(
        "id",
        "site_id",
        "kwh",
        "voltage",
        "quality",
        F.abs(F.col("voltage") - 221.8).alias("voltage_distance"),
    )
)

combined_details_site = (
    recent_readings.alias("s")
    .join(df_sites.alias("si"), F.col("si.id") == F.col("s.site_id"), "left")
    .select("s.*", F.col("si.installed_kw").alias("site_installed_kw"))
)

windowed = (
    combined_details_site
    .select(
        "*",
        F.row_number().over(w_site_id_by_site_installed_kw).alias("site_installed_kw_seq"),
        F.last("kwh", ignorenulls=True).over(w_site_id_by_voltage_running).alias("last_known_kwh"),
        F.min("site_installed_kw").over(w_site_id).alias("site_id_min_site_installed_kw"),
        F.round(F.avg("voltage_distance").over(w_site_id), 2).alias("site_id_avg_voltage_distance"),
        F.lead("kwh", 1, 0).over(w_quality_site_id_by_voltage_d).alias("next_kwh"),
        F.ntile(3).over(w_quality_site_id_by_voltage_d).alias("voltage_distance_tercile"),
        F.sum("voltage_distance").over(w_site_id_quality_by_site_inst).alias("voltage_distance_sum_4"),
        F.round(F.cume_dist().over(w_site_id_by_site_installed_kw), 3).alias("site_installed_kw_cume_dist"),
    )
)

grouped = (
    windowed
    .groupBy("quality", "site_id", "site_installed_kw_seq")
    .agg(
        F.count("*").alias("n_rows"),
        F.sum("site_id_min_site_installed_kw").alias("total_site_id_min_site_installed_kw"),
        F.array_sort(F.collect_set("quality")).alias("quality_list"),
        F.round(F.sum("last_known_kwh") / F.nullif(F.sum("site_id_min_site_installed_kw"), F.lit(0)), 3).alias("last_known_kwh_to_site_id_min_site_installed_kw_"),
        F.sum(F.when(~F.col("quality").isin("EST", "BAD"), F.col("site_id_avg_voltage_distance")).otherwise(0)).alias("site_id_avg_voltage_distance_quality_not_in_est_"),
    )
)

sequenced = (
    grouped
    .select(
        "quality",
        "site_id",
        "site_installed_kw_seq",
        "n_rows",
        "quality_list",
        "last_known_kwh_to_site_id_min_site_installed_kw_",
        F.round(F.col("n_rows") - F.avg("n_rows").over(w_site_id_n_rows), 2).alias("n_rows_vs_site_id_n_rows_avg"),
        F.round(F.col("n_rows") / F.nullif(F.sum("n_rows").over(w_site_id_n_rows), F.lit(0)), 4).alias("n_rows_share_of_site_id_n_rows"),
        F.min("n_rows").over(w_site_id_n_rows).alias("site_id_n_rows_min_n_rows"),
        F.round(F.col("site_installed_kw_seq") - F.avg("site_installed_kw_seq").over(w_site_id_n_rows), 2).alias("site_installed_kw_seq_vs_site_id_n_rows_avg"),
    )
)

result = (
    sequenced
    .select(
        "quality",
        "n_rows",
        "quality_list",
        "last_known_kwh_to_site_id_min_site_installed_kw_",
        "n_rows_vs_site_id_n_rows_avg",
        "n_rows_share_of_site_id_n_rows",
        "site_id_n_rows_min_n_rows",
        "site_installed_kw_seq_vs_site_id_n_rows_avg",
        F.substring(F.trim(F.col("quality")), 1, 2).alias("quality_prefix"),
    )
    .orderBy(
        F.col("last_known_kwh_to_site_id_min_site_installed_kw_").asc_nulls_last(),
        F.col("site_installed_kw_seq_vs_site_id_n_rows_avg").asc(),
    )
)
