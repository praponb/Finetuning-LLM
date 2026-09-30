from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_site_id = Window.partitionBy("site_id")
w_quality = Window.partitionBy("quality")
w_read_at_to_date_quality_by_k = Window.partitionBy(F.to_date("read_at"), "quality").orderBy("kwh", "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_quality_by_read_at = Window.partitionBy("quality").orderBy(F.col("read_at").desc(), "id")
w_site_id_quality_by_voltage = Window.partitionBy("site_id", "quality").orderBy(F.col("voltage").desc(), "id")
w_site_id_by_kwh_full = Window.partitionBy("site_id").orderBy("kwh", "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_read_at_month_by_kwh_moving = Window.partitionBy(F.month("read_at")).orderBy(F.col("kwh").desc_nulls_first(), "id").rowsBetween(-2, Window.currentRow)

base = (
    df_meter_readings
    .filter((F.col("site_id") >= 2) & (F.col("read_at") < F.lit("2025-02-24 10:00:00").cast("timestamp")))
    .select(
        "id",
        "site_id",
        "read_at",
        "kwh",
        "voltage",
        "quality",
    )
)

metrics = (
    base
    .select(
        "*",
        F.round(F.col("voltage") / F.nullif(F.sum("voltage").over(w_site_id), F.lit(0)), 4).alias("voltage_share_of_site_id"),
        F.count(F.lit(1)).over(w_quality).alias("rows_in_quality"),
        F.round(F.col("voltage") - F.avg("voltage").over(w_quality), 2).alias("voltage_vs_quality_avg"),
        F.last("kwh").over(w_read_at_to_date_quality_by_k).alias("last_kwh"),
        F.min("voltage").over(w_site_id).alias("site_id_min_voltage"),
        F.lead("voltage", 1, 0).over(w_quality_by_read_at).alias("next_voltage"),
        F.min("kwh").over(w_quality).alias("quality_min_kwh"),
        F.round(F.percent_rank().over(w_site_id_quality_by_voltage), 3).alias("voltage_percent_rank"),
        F.first("voltage").over(w_read_at_to_date_quality_by_k).alias("first_voltage"),
        F.round(F.col("kwh") - F.avg("kwh").over(w_site_id), 2).alias("kwh_vs_site_id_avg"),
        F.round(F.col("voltage") - F.avg("voltage").over(w_site_id), 2).alias("voltage_vs_site_id_avg"),
        F.nth_value("voltage", 2).over(w_site_id_by_kwh_full).alias("second_voltage"),
        F.round(F.avg("voltage").over(w_quality), 2).alias("quality_avg_voltage"),
        F.sum("kwh").over(w_read_at_month_by_kwh_moving).alias("kwh_sum_3"),
    )
)

result = (
    metrics
    .select(
        "id",
        "site_id",
        "read_at",
        "kwh",
        "voltage",
        "rows_in_quality",
        "voltage_vs_quality_avg",
        "last_kwh",
        "voltage_percent_rank",
        "first_voltage",
        "kwh_vs_site_id_avg",
        "voltage_vs_site_id_avg",
        "quality_avg_voltage",
        "kwh_sum_3",
        F.round(F.col("voltage_percent_rank") * 0.9, 2).alias("voltage_percent_rank_adj"),
        F.concat(F.col("quality"), F.lit("#"), F.col("rows_in_quality").cast("string")).alias("quality_ref"),
    )
    .orderBy(F.col("quality_ref").asc_nulls_last())
)
