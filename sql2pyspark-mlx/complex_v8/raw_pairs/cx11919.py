from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_quality_site_id = Window.partitionBy("quality", "site_id")
w_all_by_site_installed_kw = Window.orderBy(F.col("site_installed_kw").desc(), "id")
w_site_id_by_voltage = Window.partitionBy("site_id").orderBy(F.col("voltage").desc(), "id")
w_quality_site_id_by_site_inst = Window.partitionBy("quality", "site_id").orderBy("site_installed_kw", "id")
w_site_id_by_voltage_moving_3 = Window.partitionBy("site_id").orderBy(F.col("voltage").desc(), "id").rowsBetween(-3, Window.currentRow)
w_quality = Window.partitionBy("quality")
w_site_id_by_site_installed_kw = Window.partitionBy("site_id").orderBy(F.col("site_installed_kw").desc(), "id").rowsBetween(-6, -1)

recent_readings = (
    df_meter_readings
    .filter((F.col("quality") == "BAD") & (F.col("kwh") > 20.4))
    .select(
        "id",
        "site_id",
        "read_at",
        "voltage",
        "quality",
        F.coalesce(F.col("quality"), F.lit("UNKNOWN")).alias("quality_or_unknown"),
    )
)

combined_details_site = (
    recent_readings.alias("s")
    .join(df_sites.alias("si"), F.col("si.id") == F.col("s.site_id"), "inner")
    .select(
        "s.id",
        "s.site_id",
        "s.voltage",
        "s.quality",
        "s.quality_or_unknown",
        F.col("si.installed_kw").alias("site_installed_kw"),
        "si.site_name",
    )
)

with_windows = (
    combined_details_site
    .select(
        "id",
        "site_id",
        "voltage",
        "quality",
        "quality_or_unknown",
        "site_installed_kw",
        "site_name",
        F.max("site_installed_kw").over(w_quality_site_id).alias("quality_site_id_max_site_installed_kw"),
        F.lag("voltage", 1, 0).over(w_all_by_site_installed_kw).alias("prev_voltage"),
        F.round(F.col("voltage") / F.nullif(F.sum("voltage").over(w_quality_site_id), F.lit(0)), 4).alias("voltage_share_of_quality_site_id"),
        F.rank().over(w_site_id_by_voltage).alias("voltage_rank"),
        F.lag("site_installed_kw").over(w_quality_site_id_by_site_inst).alias("prev_site_installed_kw"),
        F.max("site_installed_kw").over(w_site_id_by_voltage_moving_3).alias("site_installed_kw_max_4"),
        F.count(F.lit(1)).over(w_quality).alias("rows_in_quality"),
        F.min("voltage").over(w_quality_site_id).alias("quality_site_id_min_voltage"),
        F.max("voltage").over(w_quality).alias("quality_max_voltage"),
        F.round(F.col("site_installed_kw") - F.avg("site_installed_kw").over(w_quality_site_id), 2).alias("site_installed_kw_vs_quality_site_id_avg"),
        F.count(F.lit(1)).over(w_quality_site_id).alias("rows_in_quality_site_id"),
        F.round(F.avg("voltage").over(w_site_id_by_site_installed_kw), 2).alias("voltage_avg_6_prior"),
        F.lead("site_installed_kw").over(w_site_id_by_voltage).alias("next_site_installed_kw"),
    )
)

top_per_group = (
    with_windows
    .filter((F.col("voltage_rank") <= 5) & (F.col("site_installed_kw") >= 93.3))
)

summary = (
    top_per_group
    .filter(F.col("prev_site_installed_kw") > 141.6)
    .groupBy("site_id", "quality", "quality_or_unknown")
    .agg(
        F.max("rows_in_quality_site_id").alias("max_rows_in_quality_site_id"),
        F.countDistinct("quality_or_unknown").alias("distinct_quality_or_unknowns"),
        F.min("voltage_share_of_quality_site_id").alias("min_voltage_share_of_quality_site_id"),
        F.avg("voltage_avg_6_prior").alias("avg_voltage_avg_6_prior_check"),
    )
    .filter(F.col("avg_voltage_avg_6_prior_check") > 215.2)
    .drop("avg_voltage_avg_6_prior_check")
)

result = (
    summary
    .filter(F.col("max_rows_in_quality_site_id") <= 17)
    .select(
        "site_id",
        "quality",
        "max_rows_in_quality_site_id",
        "distinct_quality_or_unknowns",
        "min_voltage_share_of_quality_site_id",
        F.concat(F.col("quality"), F.lit("#"), F.col("site_id").cast("string")).alias("quality_ref"),
        F.initcap(F.trim(F.col("quality"))).alias("quality_clean"),
    )
    .orderBy("min_voltage_share_of_quality_site_id")
)
