from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_site_id = Window.partitionBy("site_id")
w_site_id_quality_by_voltage_f = Window.partitionBy("site_id", "quality").orderBy("voltage", "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_quality_by_kwh_ties = Window.partitionBy("quality").orderBy("kwh")
w_quality_by_kwh = Window.partitionBy("quality").orderBy("kwh", "id")
w_site_id_quality_by_voltage_t = Window.partitionBy("site_id", "quality").orderBy("voltage")

base_readings = (
    df_meter_readings
    .filter((F.col("site_id").isNotNull()) & (F.col("read_at") >= F.lit("2025-01-19 09:00:00").cast("timestamp")))
    .select(
        "id",
        "site_id",
        "kwh",
        "voltage",
        "quality",
    )
)

with_windows = (
    base_readings
    .select(
        "*",
        F.round(F.col("kwh") / F.nullif(F.sum("kwh").over(w_site_id), F.lit(0)), 4).alias("kwh_share_of_site_id"),
        F.round(F.col("voltage") - F.avg("voltage").over(w_site_id), 2).alias("voltage_vs_site_id_avg"),
        F.nth_value("kwh", 2).over(w_site_id_quality_by_voltage_f).alias("second_kwh"),
        F.rank().over(w_quality_by_kwh_ties).alias("kwh_rank"),
        F.round(F.cume_dist().over(w_quality_by_kwh), 3).alias("kwh_cume_dist"),
        F.size(F.collect_set("quality").over(w_site_id)).alias("distinct_quality_in_site_id"),
        F.dense_rank().over(w_site_id_quality_by_voltage_t).alias("voltage_dense_rank"),
    )
)

result = (
    with_windows
    .select(
        "id",
        "site_id",
        "kwh",
        "voltage",
        "quality",
        "kwh_share_of_site_id",
        "voltage_vs_site_id_avg",
        "second_kwh",
        "kwh_rank",
        "kwh_cume_dist",
        "distinct_quality_in_site_id",
        "voltage_dense_rank",
        F.abs(F.col("voltage_vs_site_id_avg") - -98.0).alias("voltage_vs_site_id_avg_distance"),
    )
    .orderBy(F.col("site_id").desc(), "kwh")
)
