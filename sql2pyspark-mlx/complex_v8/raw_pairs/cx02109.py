from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_site_id_by_kwh_moving_6 = Window.partitionBy("site_id").orderBy("kwh", "id").rowsBetween(-6, Window.currentRow)
w_all_by_voltage_running = Window.orderBy(F.col("voltage").asc_nulls_last(), "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_site_id_by_kwh = Window.partitionBy("site_id").orderBy("kwh", "id")
w_all_by_kwh_ties = Window.orderBy("kwh")
w_site_id_read_at_month = Window.partitionBy("site_id", F.date_trunc("month", "read_at"))
w_all_by_voltage = Window.orderBy(F.col("voltage").asc_nulls_last(), "id")
w_read_at_month_by_voltage_mov = Window.partitionBy(F.month("read_at")).orderBy("voltage", "id").rowsBetween(-6, Window.currentRow)
w_site_id_read_at_month_by_vol = Window.partitionBy("site_id", F.month("read_at")).orderBy("voltage", "id")
w_site_id = Window.partitionBy("site_id")
w_read_at_month_by_voltage = Window.partitionBy(F.month("read_at")).orderBy("voltage", "id")
w_all_by_kwh = Window.orderBy("kwh", "id")

cleaned = (
    df_meter_readings
    .select(
        "id",
        "site_id",
        "read_at",
        "kwh",
        "voltage",
        (F.floor(F.col("voltage") / 5) * 5).alias("voltage_bucket"),
    )
)

windowed = (
    cleaned
    .select(
        "*",
        F.max("kwh").over(w_site_id_by_kwh_moving_6).alias("kwh_max_7"),
        F.first("kwh", ignorenulls=True).over(w_all_by_voltage_running).alias("first_known_kwh"),
        F.rank().over(w_site_id_by_kwh).alias("kwh_rank"),
        F.dense_rank().over(w_all_by_kwh_ties).alias("kwh_dense_rank"),
        F.row_number().over(w_all_by_voltage).alias("voltage_seq"),
        F.max("kwh").over(w_read_at_month_by_voltage_mov).alias("kwh_max_7_2"),
        F.lag("voltage").over(w_site_id_read_at_month_by_vol).alias("prev_voltage"),
        F.round(F.percent_rank().over(w_site_id_read_at_month_by_vol), 3).alias("voltage_percent_rank"),
        F.round(F.col("voltage_bucket") - F.avg("voltage_bucket").over(w_site_id), 2).alias("voltage_bucket_vs_site_id_avg"),
        F.lag("voltage_bucket").over(w_read_at_month_by_voltage).alias("prev_voltage_bucket"),
        F.round(F.col("voltage") / F.nullif(F.sum("voltage").over(w_site_id_read_at_month), F.lit(0)), 4).alias("voltage_share_of_site_id_read_at_month"),
        F.row_number().over(w_all_by_kwh).alias("kwh_seq"),
    )
)

result = (
    windowed
    .filter(F.col("kwh_dense_rank") > 2)
    .select("kwh_seq")
    .distinct()
    .orderBy(F.col("kwh_seq").desc())
)
