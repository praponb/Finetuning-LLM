from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_all_by_voltage = Window.orderBy("voltage", "id")
w_site_id_read_at_month_by_rea = Window.partitionBy("site_id", F.date_trunc("month", "read_at")).orderBy("read_at", "id")
w_all_by_voltage_moving_2 = Window.orderBy("voltage", "id").rowsBetween(-2, Window.currentRow)

prepared_readings = (
    df_meter_readings
    .filter(F.col("site_id") >= 3)
    .select(
        "id",
        "site_id",
        "read_at",
        "kwh",
        "voltage",
        F.date_format(F.col("read_at"), "MMM yyyy").alias("read_at_label"),
        F.round(F.col("kwh") * 1.07, 2).alias("kwh_adj"),
        F.coalesce(F.col("kwh"), F.lit(0.0)).alias("kwh_filled"),
    )
)

windowed = (
    prepared_readings
    .select(
        "*",
        F.round((F.unix_timestamp("read_at") - F.unix_timestamp(F.lag("read_at").over(w_all_by_voltage))) / 60, 1).alias("minutes_since_prev_read_at"),
        F.lead("read_at").over(w_site_id_read_at_month_by_rea).alias("next_read_at"),
        F.ntile(10).over(w_site_id_read_at_month_by_rea).alias("read_at_decile"),
        F.round(F.avg("kwh").over(w_all_by_voltage_moving_2), 2).alias("kwh_avg_3"),
    )
)

result = (
    windowed
    .groupingSets(
        [["site_id", "read_at_decile"], ["site_id"]],
        "site_id", "read_at_decile",
    )
    .agg(
        F.round(F.sum("minutes_since_prev_read_at") / F.nullif(F.sum("kwh_filled"), F.lit(0)), 3).alias("minutes_since_prev_read_at_to_kwh_filled_ratio"),
    )
    .orderBy(
        F.col("site_id").asc_nulls_last(),
        F.col("read_at_decile").asc_nulls_last(),
    )
)
