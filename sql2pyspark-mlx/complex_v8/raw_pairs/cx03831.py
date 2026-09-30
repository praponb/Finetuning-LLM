from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_quality_read_at_month_by_vol = Window.partitionBy("quality", F.month("read_at")).orderBy("voltage", "id")
w_all_by_read_at_prior_1 = Window.orderBy(F.col("read_at").desc(), "id").rowsBetween(-1, -1)
w_all_by_read_at_ties = Window.orderBy(F.col("read_at").desc())
w_read_at_month_quality_by_rea = Window.partitionBy(F.month("read_at"), "quality").orderBy("read_at", "id")
w_quality_site_id = Window.partitionBy("quality", "site_id")
w_all_by_voltage = Window.orderBy(F.col("voltage").asc_nulls_last(), "id")
w_quality_site_id_by_voltage_p = Window.partitionBy("quality", "site_id").orderBy("voltage", "id").rowsBetween(-6, -1)

recent_readings = (
    df_meter_readings
    .select(
        "id",
        "site_id",
        "read_at",
        "kwh",
        "voltage",
        "quality",
        F.abs(F.col("kwh") - 63.6).alias("kwh_distance"),
    )
)

with_windows = (
    recent_readings
    .select(
        "id",
        "site_id",
        "read_at",
        "kwh",
        "quality",
        "kwh_distance",
        F.dense_rank().over(w_quality_read_at_month_by_vol).alias("voltage_dense_rank"),
        F.round(F.avg("voltage").over(w_all_by_read_at_prior_1), 2).alias("voltage_avg_1_prior"),
        F.dense_rank().over(w_all_by_read_at_ties).alias("read_at_dense_rank"),
        F.lag("kwh", 3, 0).over(w_read_at_month_quality_by_rea).alias("prev3_kwh"),
        F.round(F.col("voltage") - F.avg("voltage").over(w_quality_site_id), 2).alias("voltage_vs_quality_site_id_avg"),
        F.sum("voltage").over(w_quality_site_id).alias("quality_site_id_sum_voltage"),
        F.ntile(3).over(w_all_by_voltage).alias("voltage_tercile"),
        F.round(F.avg("voltage").over(w_quality_site_id_by_voltage_p), 2).alias("voltage_avg_6_prior"),
    )
)

leaders = (
    with_windows
    .filter(F.col("voltage_dense_rank") <= 5)
    .drop("voltage_dense_rank")
)

result = (
    leaders
    .select(
        "id",
        "site_id",
        "read_at",
        "kwh",
        "quality",
        "kwh_distance",
        "voltage_avg_1_prior",
        "read_at_dense_rank",
        "prev3_kwh",
        "voltage_vs_quality_site_id_avg",
        "voltage_tercile",
        "voltage_avg_6_prior",
        F.hour(F.col("read_at")).alias("read_at_hour"),
        (F.col("read_at") + F.expr("INTERVAL 15 MINUTES")).alias("read_at_plus_15m"),
    )
    .orderBy(F.col("site_id").desc(), "read_at_plus_15m")
)
