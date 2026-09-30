from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_site_id_by_kwh = Window.partitionBy("site_id").orderBy(F.col("kwh").asc_nulls_last(), "id")
w_site_id = Window.partitionBy("site_id")
w_site_id_by_voltage_ties = Window.partitionBy("site_id").orderBy("voltage")
w_site_id_by_site_installed_kw = Window.partitionBy("site_id").orderBy("site_installed_kw", "id")
w_site_id_by_kwh_running = Window.partitionBy("site_id").orderBy(F.col("kwh").asc_nulls_last(), "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_site_id_by_kwh_ties = Window.partitionBy("site_id").orderBy(F.col("kwh").desc())
w_site_id_by_site_installed_kw_2 = Window.partitionBy("site_id").orderBy("site_installed_kw")

scoped = (
    df_meter_readings
    .select(
        "id",
        "site_id",
        "kwh",
        "voltage",
        "quality",
        F.greatest(F.col("kwh"), F.col("voltage")).alias("greatest_kwh_voltage"),
        F.lower(F.regexp_replace(F.trim(F.col("quality")), " ", "_")).alias("quality_slug"),
    )
)

with_details_site = (
    scoped.alias("s")
    .join(df_sites.alias("si"), F.col("si.id") == F.col("s.site_id"), "left")
    .select(
        "s.id",
        "s.site_id",
        "s.kwh",
        "s.voltage",
        "s.quality_slug",
        F.col("si.installed_kw").alias("site_installed_kw"),
    )
)

with_trends = (
    with_details_site
    .select(
        "*",
        F.dense_rank().over(w_site_id_by_kwh).alias("kwh_dense_rank"),
        F.count(F.lit(1)).over(w_site_id).alias("rows_in_site_id"),
        F.dense_rank().over(w_site_id_by_voltage_ties).alias("voltage_dense_rank"),
        F.max("kwh").over(w_site_id).alias("site_id_max_kwh"),
        F.round(F.percent_rank().over(w_site_id_by_site_installed_kw), 3).alias("site_installed_kw_percent_rank"),
        F.count(F.lit(1)).over(w_site_id_by_kwh_running).alias("running_count"),
        F.round(F.percent_rank().over(w_site_id_by_kwh_ties), 3).alias("kwh_percent_rank"),
        F.dense_rank().over(w_site_id_by_site_installed_kw_2).alias("site_installed_kw_dense_rank"),
        F.sum("site_installed_kw").over(w_site_id).alias("site_id_sum_site_installed_kw"),
        F.ntile(10).over(w_site_id_by_site_installed_kw).alias("site_installed_kw_decile"),
        F.round(F.col("site_installed_kw") - F.avg("site_installed_kw").over(w_site_id), 2).alias("site_installed_kw_vs_site_id_avg"),
        F.round(F.col("site_installed_kw") / F.nullif(F.sum("site_installed_kw").over(w_site_id), F.lit(0)), 4).alias("site_installed_kw_share_of_site_id"),
    )
)

per_group = (
    with_trends
    .filter(F.col("kwh_dense_rank") <= 6)
    .groupBy("site_id", "site_installed_kw_dense_rank")
    .agg(
        F.count("*").alias("row_count"),
        F.countDistinct("site_id").alias("distinct_site_ids"),
        F.percentile_approx("site_installed_kw_vs_site_id_avg", 0.5).alias("median_site_installed_kw_vs_site_id_avg"),
        F.max("running_count").alias("max_running_count_check"),
    )
    .filter(F.col("max_running_count_check") > 9)
    .drop("max_running_count_check")
)

result = (
    per_group
    .select(
        "site_id",
        "row_count",
        "distinct_site_ids",
        "median_site_installed_kw_vs_site_id_avg",
    )
    .orderBy(F.col("row_count").desc())
)
