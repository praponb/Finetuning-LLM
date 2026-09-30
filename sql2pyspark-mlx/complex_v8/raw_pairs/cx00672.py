from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_quality = Window.partitionBy("quality")
w_quality_read_at_year = Window.partitionBy("quality", F.year("read_at"))
w_all_by_kwh_minus_voltage_tie = Window.orderBy("kwh_minus_voltage")
w_quality_site_id_by_voltage_r = Window.partitionBy("quality", "site_id").orderBy("voltage", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_all_by_read_at = Window.orderBy("read_at", "id")

prepared_readings = (
    df_meter_readings
    .filter((F.col("site_id").isNotNull()) & (F.col("read_at") < F.lit("2025-02-18 07:00:00").cast("timestamp")))
    .select(
        "id",
        "site_id",
        "read_at",
        "voltage",
        "quality",
        F.initcap(F.trim(F.col("quality"))).alias("quality_clean"),
        (F.col("kwh") - F.col("voltage")).alias("kwh_minus_voltage"),
    )
)

with_trends = (
    prepared_readings
    .select(
        "*",
        F.sum("voltage").over(w_quality).alias("quality_sum_voltage"),
        F.count(F.lit(1)).over(w_quality_read_at_year).alias("rows_in_quality_read_at_year"),
        F.count(F.lit(1)).over(w_quality).alias("rows_in_quality"),
        F.round(F.cume_dist().over(w_all_by_kwh_minus_voltage_tie), 3).alias("kwh_minus_voltage_cume_dist"),
        F.min("voltage").over(w_quality_site_id_by_voltage_r).alias("min_voltage_so_far"),
        F.sum("kwh_minus_voltage").over(w_quality).alias("quality_sum_kwh_minus_voltage"),
        F.round(F.col("voltage") - F.avg("voltage").over(w_quality_read_at_year), 2).alias("voltage_vs_quality_read_at_year_avg"),
        F.dense_rank().over(w_all_by_read_at).alias("read_at_dense_rank"),
    )
)

result = (
    with_trends
    .select(
        "id",
        "site_id",
        "read_at",
        "voltage",
        "quality",
        "quality_clean",
        "kwh_minus_voltage",
        "quality_sum_voltage",
        "rows_in_quality_read_at_year",
        "rows_in_quality",
        "min_voltage_so_far",
        "quality_sum_kwh_minus_voltage",
        "voltage_vs_quality_read_at_year_avg",
        "read_at_dense_rank",
        (F.floor(F.col("quality_sum_voltage") / 500) * 500).alias("quality_sum_voltage_bucket"),
        (F.col("voltage") + F.col("kwh_minus_voltage")).alias("voltage_plus_kwh_minus_voltage"),
    )
    .orderBy(F.col("quality_sum_kwh_minus_voltage").desc())
)
