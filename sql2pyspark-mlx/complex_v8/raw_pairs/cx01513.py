from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_site_id_quality = Window.partitionBy("site_id", "quality")
w_site_id_quality_by_total_kwh = Window.partitionBy("site_id", "quality").orderBy("total_kwh")

valid_readings = (
    df_meter_readings
    .filter(F.col("site_id").isNotNull())
    .select(
        "id",
        "site_id",
        "kwh",
        "voltage",
        "quality",
        F.month(F.col("read_at")).alias("read_at_month"),
        (F.floor(F.col("voltage") / 5) * 5).alias("voltage_bucket"),
        F.substring(F.trim(F.col("quality")), 1, 4).alias("quality_prefix"),
    )
)

screened = (
    valid_readings.alias("s")
    .join(df_sites.alias("si"), (F.col("si.id") == F.col("s.site_id")) & (F.col("si.zone") == "Z4") & (F.col("si.tariff").isin("TOU", "FLAT")), "left_semi")
)

rollup_base = (
    screened
    .filter(F.col("quality_prefix").isNotNull())
    .groupBy("quality", "site_id")
    .agg(
        F.sum("kwh").alias("total_kwh"),
        F.array_sort(F.collect_set("quality")).alias("quality_list"),
        F.sum("read_at_month").alias("sum_read_at_month_check"),
    )
    .filter(F.col("sum_read_at_month_check") > 2)
    .drop("sum_read_at_month_check")
)

windowed = (
    rollup_base
    .select(
        "quality",
        "site_id",
        "total_kwh",
        "quality_list",
        F.round(F.col("total_kwh") - F.avg("total_kwh").over(w_site_id_quality), 2).alias("total_kwh_vs_site_id_quality_avg"),
        F.min("total_kwh").over(w_site_id_quality).alias("site_id_quality_min_total_kwh"),
        F.dense_rank().over(w_site_id_quality_by_total_kwh).alias("total_kwh_dense_rank"),
        F.round(F.cume_dist().over(w_site_id_quality_by_total_kwh), 3).alias("total_kwh_cume_dist"),
    )
)

result = (
    windowed
    .select(
        "quality",
        "total_kwh",
        "quality_list",
        "site_id_quality_min_total_kwh",
        "total_kwh_dense_rank",
        "total_kwh_cume_dist",
    )
    .orderBy(
        F.col("total_kwh").desc(),
        F.col("total_kwh_dense_rank").asc_nulls_last(),
    )
)
