from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_all_by_kwh_moving_3 = Window.orderBy(F.col("kwh").asc_nulls_last(), "id").rowsBetween(-3, Window.currentRow)
w_site_id_by_kwh_prior_3 = Window.partitionBy("site_id").orderBy(F.col("kwh").desc(), "id").rowsBetween(-3, -1)
w_site_id_by_kwh_full = Window.partitionBy("site_id").orderBy(F.col("kwh").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_site_id_by_kwh_ties = Window.partitionBy("site_id").orderBy(F.col("kwh").desc())
w_quality_site_id = Window.partitionBy("quality", "site_id")
w_site_id = Window.partitionBy("site_id")
w_site_id_by_voltage_full = Window.partitionBy("site_id").orderBy("voltage", "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_site_id_by_kwh_running = Window.partitionBy("site_id").orderBy(F.col("kwh").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

filtered_readings = (
    df_meter_readings
    .filter(F.col("quality").isNotNull())
    .select(
        "id",
        "site_id",
        "read_at",
        "kwh",
        "voltage",
        "quality",
    )
)

sequenced = (
    filtered_readings
    .select(
        "id",
        "kwh",
        "voltage",
        "quality",
        F.max("kwh").over(w_all_by_kwh_moving_3).alias("kwh_max_4"),
        F.round(F.avg("kwh").over(w_site_id_by_kwh_prior_3), 2).alias("kwh_avg_3_prior"),
        F.first("read_at").over(w_site_id_by_kwh_full).alias("first_read_at"),
        F.round(F.percent_rank().over(w_site_id_by_kwh_ties), 3).alias("kwh_percent_rank"),
        F.round(F.col("voltage") - F.avg("voltage").over(w_quality_site_id), 2).alias("voltage_vs_quality_site_id_avg"),
        F.max("voltage").over(w_site_id).alias("site_id_max_voltage"),
        F.nth_value("voltage", 2).over(w_site_id_by_kwh_full).alias("second_voltage"),
        F.nth_value("voltage", 2).over(w_site_id_by_voltage_full).alias("second_voltage_2"),
        F.sum("kwh").over(w_site_id_by_kwh_running).alias("running_kwh"),
    )
)

result = (
    sequenced
    .select(
        "id",
        "voltage",
        "kwh_max_4",
        "first_read_at",
        "site_id_max_voltage",
        "second_voltage_2",
        "running_kwh",
        F.substring(F.trim(F.col("quality")), 1, 4).alias("quality_prefix"),
    )
    .orderBy(F.col("first_read_at").asc(), F.col("site_id_max_voltage").desc())
)
